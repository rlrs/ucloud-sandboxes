#!/usr/bin/env python3
"""Qualify local immutable EROFS images on the host and the pinned runtime.

Builds an allowlisted fixture, never an execution snapshot. This is a functional
gate, not a demand-loading or performance result. Run as root on an isolated VM.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runsc', type=Path, required=True)
    parser.add_argument('--busybox', type=Path, default=Path('/usr/bin/busybox'))
    parser.add_argument('--work-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--profile', choices=('full', 'no-xattrs'), default='full',
                        help='no-xattrs tests only the constrained native toolkit profile')
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('requires root on an isolated qualification VM')
    result = {'runsc_sha256': hashlib.sha256(args.runsc.read_bytes()).hexdigest(),
              'phases': [], 'passed': False, 'profile': args.profile,
              'scope': 'local uncompressed immutable image; no range loading'}
    root = Path(tempfile.mkdtemp(prefix='erofs-qualification-', dir=args.work_root))
    mounted = []

    def command(*argv):
        start = time.monotonic()
        completed = subprocess.run([str(a) for a in argv], text=True, capture_output=True, timeout=90)
        result['phases'].append({'command': [str(a) for a in argv],
            'seconds': time.monotonic() - start, 'returncode': completed.returncode,
            'stdout': completed.stdout[-4000:], 'stderr': completed.stderr[-4000:]})
        completed.check_returncode()
        return completed

    try:
        source = root / 'source'
        source.mkdir()
        for name in ('bin', 'data', 'dev', 'proc', 'sys', 'tmp'):
            (source / name).mkdir()
        shutil.copyfile(args.busybox, source / 'bin/busybox')
        (source / 'bin/busybox').chmod(0o755)
        (source / 'data/original').write_text('immutable\n')
        if args.profile == 'full':
            os.setxattr(source / 'data/original', 'user.qualification', b'preserved')
        os.link(source / 'data/original', source / 'data/hardlink')
        (source / 'data/symlink').symlink_to('original')
        image = root / 'fixture.erofs'
        # The pinned gVisor mapper only supports flat (non-inline) file data.
        command('mkfs.erofs', '-T0', '-E', 'noinline_data', image, source)
        digest = hashlib.sha256(image.read_bytes()).hexdigest()
        result['image_sha256'] = digest
        result['image_bytes'] = image.stat().st_size
        lower = root / 'lower'
        lower.mkdir()
        command('mount', '-t', 'erofs', '-o', 'loop,ro', image, lower)
        mounted.append(lower)
        assert (lower / 'data/symlink').read_text() == 'immutable\n'
        assert os.stat(lower / 'data/original').st_ino == os.stat(lower / 'data/hardlink').st_ino
        if args.profile == 'full':
            assert os.getxattr(lower / 'data/original', 'user.qualification') == b'preserved'
        try:
            (lower / 'data/original').write_text('must fail')
        except OSError:
            pass
        else:
            raise AssertionError('EROFS lower was writable')
        result['host_semantics'] = True
        for path in ('upper', 'work', 'merged'):
            (root / path).mkdir()
        command('mount', '-t', 'overlay', 'overlay', '-o',
                f'lowerdir={lower},upperdir={root / "upper"},workdir={root / "work"}', root / 'merged')
        mounted.append(root / 'merged')
        (root / 'merged/data/original').write_text('copy-up\n')
        (root / 'merged/data/hardlink').unlink()
        assert (lower / 'data/original').read_text() == 'immutable\n'
        assert not (root / 'merged/data/hardlink').exists()
        result['host_copy_up_and_whiteout'] = True

        bundle = root / 'bundle'
        bundle.mkdir()
        (bundle / 'rootfs').mkdir()
        (root / 'runtime-upper').mkdir()
        # Independent disk backing for the writable gVisor overlay. A memory
        # overlay would hide the very density cost this qualification targets.
        config = {
            'ociVersion': '1.0.2', 'root': {'path': 'rootfs', 'readonly': False},
            'annotations': {'dev.gvisor.spec.rootfs.type': 'erofs',
                'dev.gvisor.spec.rootfs.source': str(image),
                'dev.gvisor.spec.rootfs.overlay': 'dir=' + str(root / 'runtime-upper')},
            'process': {'terminal': False, 'user': {'uid': 0, 'gid': 0},
                'args': ['/bin/busybox', 'sh', '-ec',
                    'test "$(/bin/busybox cat /data/symlink)" = immutable; '
                    'test /data/original -ef /data/hardlink; '
                    'echo cow > /data/original; /bin/busybox rm /data/hardlink; '
                    'test ! -e /data/hardlink; test "$(/bin/busybox cat /data/original)" = cow; '
                    'echo EROFS_NATIVE_OK'],
                'env': ['PATH=/bin'], 'cwd': '/', 'noNewPrivileges': True,
                'capabilities': {name: [] for name in ('bounding', 'effective', 'inheritable', 'permitted')}},
            'mounts': [{'destination': '/proc', 'type': 'proc', 'source': 'proc',
                        'options': ['nosuid', 'noexec', 'nodev']}],
            'linux': {'namespaces': [{'type': kind} for kind in ('pid', 'ipc', 'uts', 'mount', 'network')]},
        }
        (bundle / 'config.json').write_text(json.dumps(config))
        state = ['--root=' + str(root / 'runsc')]
        try:
            native = command(args.runsc, *state, '--debug', '--debug-log=' + str(root / 'debug-'),
                             '--network=none', 'run', '--bundle=' + str(bundle), 'erofs-probe')
            assert 'EROFS_NATIVE_OK' in native.stdout
            result['native_disk_overlay'] = True
        finally:
            command(args.runsc, *state, 'delete', '--force', 'erofs-probe')
        assert hashlib.sha256(image.read_bytes()).hexdigest() == digest
        result['passed'] = True
    except Exception as exc:
        result['error'] = str(exc)
    finally:
        result['runtime_logs'] = {path.name: path.read_text(errors='replace')[:131072]
                                  for path in root.glob('debug-*') if path.is_file()}
        cleanup_errors = []
        for target in reversed(mounted):
            try:
                command('umount', target)
            except Exception as exc:
                cleanup_errors.append(str(exc))
        result['cleanup_errors'] = cleanup_errors
        if not cleanup_errors:
            shutil.rmtree(root)
        else:
            result['passed'] = False
            result['retained_work_root'] = str(root)
        args.output.write_text(json.dumps(result, indent=2) + '\n')
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
