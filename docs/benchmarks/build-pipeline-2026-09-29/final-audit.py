#!/usr/bin/env python3
"""Read-only final audit of the pinned production build-pipeline release."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import stat
import subprocess
import zipfile

ROOT = Path('/work/ucloud-sandboxes/build-pipeline-20260929-r2')
WHEEL_SHA = '405516727b99eed510a0bcfcee39203cb69f67a4b56ad7c68bc5d3eaa672e628'
CONTROLLER_SHA = 'dc0f92fcb13eb946e5dbc2b453a78e726f7bd6347b82c6ce7fd35b0f8ed4b184'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    controller = ROOT / 'deployment-controller.py'
    if sha(controller) != CONTROLLER_SHA:
        raise ValueError('Controller changed')
    spec = importlib.util.spec_from_file_location('deployment', controller)
    deploy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(deploy)
    raw, config = deploy.read_config()
    deploy.require(raw['node_package_root'] == str(ROOT), 'Node package source changed')
    deploy.require(config.builder.max_finishing_builds == 2, 'Pipeline policy changed')
    deploy.require(config.builder.build_execution_timeout_seconds == 1800,
                   'Execution budget changed')
    deploy.require(config.builder.buildx_cache_max_entries == 512 and
                   config.builder.buildx_cache_max_bytes == 32 * 1024**3,
                   'Shared cache budget changed')
    config_stat = deploy.CONFIG.stat()
    deploy.require(stat.S_IMODE(config_stat.st_mode) == 0o644 and
                   config_stat.st_uid == config_stat.st_gid == 0,
                   'Expected service-readable root-owned configuration')
    wheel = ROOT / 'ucloud_sandboxes-0.7.0-py3-none-any.whl'
    deploy.require(sha(wheel) == WHEEL_SHA, 'Wheel changed')
    import ucloud_sandboxes
    site = Path(ucloud_sandboxes.__file__).parent.parent
    checked = 0
    with zipfile.ZipFile(wheel) as archive:
        for item in archive.infolist():
            if item.filename.startswith('ucloud_sandboxes/') and not item.is_dir():
                deploy.require((site / item.filename).read_bytes() == archive.read(item),
                               'Installed package differs from pinned wheel')
                checked += 1
    staging = json.loads((ROOT / 'staging-receipt.json').read_text())
    bundle_hashes = {}
    for role in ('builder', 'sandbox'):
        path = ROOT / (role + '-node-package.tar.gz')
        digest = sha(path)
        deploy.require(digest == staging['bundles'][role]['sha256'],
                       'Node bundle changed')
        bundle_hashes[role] = digest
    services = {name: subprocess.check_output(['systemctl', 'is-active', name], text=True).strip()
                for name in deploy.SERVICES}
    upgrade_units = {}
    for name in ('apt-daily.timer', 'apt-daily-upgrade.timer', 'apt-daily.service',
                 'apt-daily-upgrade.service', 'unattended-upgrades.service'):
        value = subprocess.run(['systemctl', 'is-enabled', name], capture_output=True,
                               text=True, check=False).stdout.strip()
        deploy.require(value == 'masked', 'Automatic upgrade policy changed')
        upgrade_units[name] = value
    sandboxes = deploy.request_json('/v1/sandboxes')['sandboxes']
    builds = deploy.request_json('/v1/images/builds')['builds']
    prepared = deploy.request_json('/v1/builders/prepare')['prepared_builders']
    deploy.require(all(isinstance(value, list) for value in (sandboxes, builds, prepared)),
                   'Unexpected fleet API response shape')
    with sqlite3.connect(config.control_state_file().resolve().as_uri() + '?mode=ro', uri=True) as db:
        fleet_count = db.execute("SELECT COUNT(*) FROM control_records WHERE namespace='heartbeat'").fetchone()[0]
    active = sum(row['status'] not in {'succeeded', 'failed'} for row in builds)
    deploy.require(not sandboxes and not active and not prepared and not fleet_count,
                   'Fleet is not yet fully idle and released; inspect before retrying')
    result = dict(verified_at=deploy.stamp(), wheel_sha256=WHEEL_SHA,
                  runtime_source_commit='3cdae92b546d634c804035e3c6a9efd00ed2441e',
                  installed_files_match_wheel=checked, node_package_root=str(ROOT),
                  node_bundle_sha256=bundle_hashes, max_preparing_solving=4,
                  max_finishing_builds=2, max_owned_builds=6, buildkit_parallelism=4,
                  builder_execution_timeout_seconds=1800, cache_max_entries=512,
                  cache_max_bytes=32 * 1024**3,
                  configuration_mode=oct(stat.S_IMODE(deploy.CONFIG.stat().st_mode)),
                  fleet_nodes=fleet_count, prepared_builders=len(prepared),
                  active_builds=active, sandboxes=len(sandboxes), services=services,
                  automatic_upgrade_units=upgrade_units, health=deploy.health())
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
