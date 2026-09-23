#!/usr/bin/env python3
"""Compare XFS journal sizes on owned disposable loop devices; run as root.

Never accepts an existing device/image. Every run creates a new sparse image,
records its loop association, unmounts it, and detaches only that owned loop.
"""
import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import shutil
import subprocess
import tempfile
import time


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT)


def percentile(values, fraction):
    return sorted(values)[int((len(values) - 1) * fraction)]


def cpu_evidence():
    relative = next(line.split(':', 2)[2] for line in
                    Path('/proc/self/cgroup').read_text().splitlines()
                    if line.startswith('0::'))
    root = Path('/sys/fs/cgroup') / relative.lstrip('/')
    return dict(cgroup=relative, cpu_max=(root / 'cpu.max').read_text().strip(),
                cpu_stat={key: int(value) for key, value in
                          (line.split() for line in (root / 'cpu.stat').read_text().splitlines())})


def workload(root, seconds):
    db = sqlite3.connect(root / 'repository.sqlite')
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.execute('CREATE TABLE changes (id INTEGER PRIMARY KEY, value BLOB)')
    db.commit()
    payload = os.urandom(64 * 1024)
    cpu_before = cpu_evidence()
    started = time.monotonic()
    latencies = []
    iteration = 0
    while time.monotonic() - started < seconds:
        if iteration % 32 == 0:
            for index in range(64):
                (root / f'file-{index}').write_bytes(payload)
        before = time.monotonic()
        with db:
            db.execute('INSERT OR REPLACE INTO changes VALUES (?, ?)',
                       (iteration % 512, payload))
        latencies.append(time.monotonic() - before)
        iteration += 1
    assert db.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
    count = db.execute('SELECT count(*) FROM changes').fetchone()[0]
    db.close()
    return dict(seconds=time.monotonic() - started, transactions=iteration,
                transactions_per_second=iteration / (time.monotonic() - started),
                p95_seconds=percentile(latencies, .95),
                p99_seconds=percentile(latencies, .99), rows=count,
                cpu_before=cpu_before, cpu_after=cpu_evidence(),
                payload_sha256=hashlib.sha256(payload).hexdigest())


def qualify(root, *, label, log_mb, seconds, size_gib, log_concurrency=None, sector_size=4096):
    raw = tempfile.mkdtemp(prefix=f'{label}-', dir=root)
    trial = Path(raw)
    image = trial / 'workspace.img'
    with image.open('xb') as handle:
        handle.truncate(size_gib * 1024**3)
    loop = command('losetup', '--find', '--show', '--sector-size', str(sector_size), str(image)).strip()
    mount = trial / 'mount'
    mount.mkdir()
    mounted = False
    try:
        stat = Path('/sys/class/block') / Path(loop).name / 'stat'
        before = list(map(int, stat.read_text().split()))
        started = time.monotonic()
        args = ['mkfs.xfs', '-f', '-m', 'reflink=1', '-n', 'ftype=1']
        if log_mb is not None:
            args += ['-l', f'size={log_mb}m']
        elif log_concurrency is not None:
            args += ['-l', f'concurrency={log_concurrency}']
        formatting = command(*args, loop)
        format_seconds = time.monotonic() - started
        after = list(map(int, stat.read_text().split()))
        command('mount', '-t', 'xfs', '-o', 'noatime,nouuid', loop, str(mount))
        mounted = True
        result = workload(mount, seconds)
        command('umount', str(mount))
        mounted = False
        command('mount', '-t', 'xfs', '-o', 'noatime,nouuid', loop, str(mount))
        mounted = True
        with closing(sqlite3.connect(mount / 'repository.sqlite')) as db:
            assert db.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
            assert db.execute('SELECT count(*) FROM changes').fetchone()[0] == result['rows']
            assert all(hashlib.sha256(value).hexdigest() == result['payload_sha256']
                       for (value,) in db.execute('SELECT value FROM changes'))
        assert all(hashlib.sha256((mount / f'file-{i}').read_bytes()).hexdigest()
                   == result['payload_sha256'] for i in range(64))
        command('umount', str(mount))
        mounted = False
        repair = command('xfs_repair', '-n', loop)
        final = list(map(int, stat.read_text().split()))
        return dict(label=label, log_mb=log_mb, log_concurrency=log_concurrency, sector_size=sector_size, size_gib=size_gib,
                    format_seconds=format_seconds,
                    format_write_bytes=(after[6]-before[6])*512,
                    total_write_bytes=(final[6]-before[6])*512,
                    formatting=formatting, workload=result, repair=repair)
    finally:
        if mounted:
            command('umount', str(mount))
        # Refuse to detach a reused/unrelated device even during cleanup.
        backing = command('losetup', '--noheadings', '--output', 'BACK-FILE', loop).strip()
        if Path(backing).resolve() != image.resolve():
            raise RuntimeError('qualification loop ownership changed')
        command('losetup', '-d', loop)
        shutil.rmtree(trial)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--seconds', type=float, default=15)
    parser.add_argument('--size-gib', type=int, default=4)
    parser.add_argument('--host-concurrency', type=int, default=32)
    parser.add_argument('--sector-size', type=int, choices=(512, 4096), default=4096)
    args = parser.parse_args()
    if os.geteuid() != 0 or not 1 <= args.seconds <= 60 or not 1 <= args.size_gib <= 64:
        parser.error('requires root, duration 1..60 seconds and size 1..64 GiB')
    args.root.mkdir(mode=0o700, parents=True, exist_ok=True)
    print(json.dumps(dict(kernel=command('uname', '-r').strip(),
                          mkfs=command('mkfs.xfs', '-V').strip(),
                          cpu=cpu_evidence())), flush=True)
    modes = [('host', None, args.host_concurrency), ('filesystem', None, 0),
             ('log128m', 128, None)]
    for label, log_mb, concurrency in [*modes, *reversed(modes)]:
        print(json.dumps(qualify(args.root, label=label, log_mb=log_mb,
                                seconds=args.seconds, size_gib=args.size_gib,
                                log_concurrency=concurrency, sector_size=args.sector_size)), flush=True)


if __name__ == '__main__':
    main()
