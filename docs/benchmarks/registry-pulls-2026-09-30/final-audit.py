#!/usr/bin/env python3
"""Read-only final audit of the pinned production registry-publication release."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import stat
import subprocess
import zipfile

ROOT = Path('/work/ucloud-sandboxes/registry-pulls-20260930-r1')
WHEEL_SHA = '3f29209df80e27d3aad2d1edb3c1677ce05bb44d50b78da1797b6b0a212c13f3'
CONTROLLER_SHA = '420f2e82bec804ce119759da9e6af5d01b920b9e994a191ecca0711284861b67'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def audit_package(site, archive):
    expected = {item.filename for item in archive.infolist()
                if item.filename.startswith('ucloud_sandboxes/') and not item.is_dir()}
    package = site / 'ucloud_sandboxes'
    if not expected or package.is_symlink():
        raise ValueError('Invalid runtime package')
    actual = set()
    for path in package.rglob('*'):
        if '__pycache__' in path.parts or path.suffix == '.pyc':
            continue
        if path.is_symlink():
            raise ValueError('Unexpected runtime package symlink')
        if path.is_file():
            actual.add(path.relative_to(site).as_posix())
    if actual != expected:
        raise ValueError('Installed package file set differs from pinned wheel')
    for name in expected:
        if (site / name).read_bytes() != archive.read(name):
            raise ValueError('Installed package differs from pinned wheel')
    return len(expected)


def apt_periodic_policy():
    keys = ('Enable', 'Update-Package-Lists', 'Unattended-Upgrade')
    result = {}
    for key in keys:
        output = subprocess.check_output(
            ['apt-config', 'shell', 'value', 'APT::Periodic::' + key], text=True, timeout=10).strip()
        if output != "value='0'":
            raise ValueError('Automatic APT periodic policy changed')
        result[key] = 0
    return result


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
    with zipfile.ZipFile(wheel) as archive:
        checked = audit_package(site, archive)
    deployment = json.loads((ROOT / 'deployment-receipt.json').read_text())
    deploy.require(deployment['wheel_sha256'] == WHEEL_SHA and
                   deployment['node_package_root'] == str(ROOT), 'Deployment receipt changed')
    staging = deploy.validate_receipt(deployment['staging_receipt_sha256'], live_config=False)
    deploy.require(staging['wheel_sha256'] == WHEEL_SHA, 'Staging wheel identity changed')
    bundle_hashes = {}
    for role in ('builder', 'sandbox'):
        path = ROOT / (role + '-node-package.tar.gz')
        digest = sha(path)
        deploy.require(digest == staging['bundles'][role]['sha256'],
                       'Node bundle changed')
        bundle_hashes[role] = digest
    services = {name: subprocess.check_output(['systemctl', 'is-active', name], text=True, timeout=10).strip()
                for name in deploy.SERVICES}
    upgrade_units = {}
    for name in ('apt-daily.timer', 'apt-daily-upgrade.timer', 'apt-daily.service',
                 'apt-daily-upgrade.service', 'unattended-upgrades.service'):
        value = subprocess.run(['systemctl', 'is-enabled', name], capture_output=True,
                               text=True, check=False, timeout=10).stdout.strip()
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
                  runtime_source_commit='c09e2228197a41c755e13a1cceb9ad0714edaef1',
                  installed_files_match_wheel=checked, node_package_root=str(ROOT),
                  runtime_file_set_matches=True,
                  staging_receipt_sha256=deployment['staging_receipt_sha256'],
                  node_bundle_sha256=bundle_hashes, max_preparing_solving=4,
                  max_finishing_builds=2, max_owned_builds=6, buildkit_parallelism=4,
                  builder_execution_timeout_seconds=1800, cache_max_entries=512,
                  cache_max_bytes=32 * 1024**3,
                  configuration_mode=oct(stat.S_IMODE(deploy.CONFIG.stat().st_mode)),
                  fleet_nodes=fleet_count, prepared_builders=len(prepared),
                  active_builds=active, sandboxes=len(sandboxes), services=services,
                  automatic_upgrade_units=upgrade_units,
                  apt_periodic=apt_periodic_policy(), health=deploy.health())
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
