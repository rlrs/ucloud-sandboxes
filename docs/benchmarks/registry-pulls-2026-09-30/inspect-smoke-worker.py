#!/usr/bin/env python3
"""Read-only audit of one exact owned smoke worker and automatic-update policy.

Reads the configured launcher's package path and the configured node token file;
does not inspect process environments or emit process arguments/credentials.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
from urllib.request import Request, urlopen
import zipfile


def require(value, message):
    if not value:
        raise ValueError(message)


def output(*args):
    return subprocess.check_output(args, text=True, timeout=15).strip()


def flag(argv, name):
    require(argv.count(name) == 1, 'Expected one configured argument')
    return argv[argv.index(name) + 1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-job-id', required=True)
    parser.add_argument('--expected-node-epoch', required=True)
    parser.add_argument('--wheel', type=Path, required=True)
    parser.add_argument('--wheel-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(os.geteuid() == 0 and not args.output.exists(), 'Root and a new receipt are required')
    require(hashlib.sha256(args.wheel.read_bytes()).hexdigest() == args.wheel_sha256, 'Wheel identity differs')
    service = 'ucloud-sandbox-node.service'
    pid = int(output('systemctl', 'show', service, '--property=MainPID', '--value'))
    require(pid > 0, 'Worker service is not running')
    argv = Path(f'/proc/{pid}/cmdline').read_bytes().rstrip(b'\0').decode().split('\0')
    require(argv[:4] == ['/usr/bin/python3', '-m', 'ucloud_sandboxes.cli', 'serve-direct-node-agent']
            and flag(argv, '--job-id') == args.expected_job_id, 'Expected exact owned sandbox worker')
    cwd = Path(f'/proc/{pid}/cwd').resolve(strict=True)
    launcher = cwd / 'bin/ucloud-sandboxes'
    lines = launcher.read_text().splitlines()
    require(len(lines) == 2 and lines[0] == '#!/bin/sh', 'Unexpected agent launcher')
    tokens = shlex.split(lines[1])
    require(len(tokens) == 7 and tokens[:2] == ['exec', 'env'] and tokens[2].startswith('PYTHONPATH=')
            and tokens[3:] == ['/usr/bin/python3', '-m', 'ucloud_sandboxes.cli', '$@'], 'Unexpected launch contract')
    raw_source = Path(tokens[2].split('=', 1)[1])
    require(not raw_source.is_symlink(), 'Runtime directory is unexpectedly symlinked')
    source = raw_source.resolve(strict=True)
    require(str(source).startswith('/var/cache/ucloud-sandboxes/init-packages/')
            and not (cwd / 'ucloud_sandboxes').exists(), 'Unexpected package path or current-directory shadow')
    with zipfile.ZipFile(args.wheel) as archive:
        expected = {v.filename: archive.read(v) for v in archive.infolist()
                    if not v.is_dir() and v.filename.startswith('ucloud_sandboxes/')}
    package = source / 'ucloud_sandboxes'
    require(not package.is_symlink() and all(not p.is_symlink() for p in package.rglob('*')),
            'Runtime package contains a symlink')
    paths = [p for p in package.rglob('*') if p.is_file()
             and '__pycache__' not in p.parts and p.suffix != '.pyc']
    require(expected and {p.relative_to(source).as_posix() for p in paths} == set(expected),
            'Runtime file set differs')
    require(all(not p.is_symlink() and p.read_bytes() == expected[p.relative_to(source).as_posix()] for p in paths),
            'Installed package bytes differ')
    # Resolve imports using the exact launcher's explicit path; execute no app code.
    program = ("import importlib.machinery,json,sys;"
               "p=importlib.machinery.PathFinder.find_spec('ucloud_sandboxes',sys.path);"
               "m=importlib.machinery.PathFinder.find_spec('ucloud_sandboxes.cli',p.submodule_search_locations);"
               "print(json.dumps([p.origin,m.origin]))")
    origins = subprocess.check_output(['/usr/bin/python3', '-c', program], cwd=cwd,
        env={'PATH': '/usr/bin:/bin', 'PYTHONPATH': str(source)}, text=True, timeout=10)
    require(json.loads(origins) == [str(source / 'ucloud_sandboxes' / name) for name in ('__init__.py', 'cli.py')],
            'Launcher imports differ')
    token = Path(flag(argv, '--node-control-bearer-token-file')).read_text().strip()
    request = Request(flag(argv, '--node-url').rstrip('/') + '/v1/heartbeat',
                      headers={'Authorization': 'Bearer ' + token})
    with urlopen(request, timeout=10) as response:
        body = response.read(1024 * 1024 + 1)
    require(len(body) <= 1024 * 1024, 'Oversized heartbeat')
    heartbeat = json.loads(body)['heartbeat']
    require(heartbeat['job_id'] == args.expected_job_id and heartbeat['node_epoch'] == args.expected_node_epoch,
            'Live worker incarnation differs')
    units = {}
    for unit in ('apt-daily.timer', 'apt-daily-upgrade.timer', 'apt-daily.service',
                 'apt-daily-upgrade.service', 'unattended-upgrades.service'):
        values = dict(line.split('=', 1) for line in output('systemctl', 'show', unit,
            '--property=UnitFileState', '--property=ActiveState').splitlines())
        require(values == {'UnitFileState': 'masked', 'ActiveState': 'inactive'}, 'Automatic updates are enabled')
        units[unit] = values
    periodic = {}
    for key in ('Enable', 'Update-Package-Lists', 'Unattended-Upgrade'):
        raw = output('apt-config', 'shell', 'value', 'APT::Periodic::' + key)
        require(shlex.split(raw) == ['value=0'], 'Effective APT periodic policy differs')
        periodic[key] = 0
    require(int(output('systemctl', 'show', service, '--property=MainPID', '--value')) == pid,
            'Worker restarted during audit')
    result = dict(complete=True, at=datetime.now(timezone.utc).isoformat(),
        worker={k: heartbeat.get(k) for k in ('job_id', 'node_id', 'node_epoch', 'node_url')},
        wheel_sha256=args.wheel_sha256, installed_files_match_wheel=len(expected),
        runtime_file_set_matches=True, launcher_import_origins_match=True,
        runtime_root=str(source), pid=pid, automatic_upgrade_units=units, apt_periodic=periodic,
        helper_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), service_changed=False)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'complete': False, 'error_type': type(error).__name__}))
        raise SystemExit(1) from None
