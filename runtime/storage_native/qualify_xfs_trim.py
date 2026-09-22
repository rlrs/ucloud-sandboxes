#!/usr/bin/env python3
"""Qualify bounded XFS FITRIM through a private native ublk daemon.

Run as root on an idle Linux worker authorized for qualification. Creates only private devices
and mounts, never trims an existing volume. Automatic runtime trim stays disabled
until this test passes with the deployment's exact backend artifact and kernel.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
import threading

from qualify_volume import Qualifier
from ucloud_sandboxes.storage_native_registry import consume_export_stream

MIB = 1024**2


class TrimQualifier(Qualifier):
    def device(self, name, layers):
        source = self.test_root / f'{name}.json'
        self._write_json(source, {'lowers': [{'file': str(p)} for p in layers],
                                 'upper': {}, 'repoBlobUrl': '', 'resultFile': ''})
        device = self.client.create_runtime_device(source_image_config=source,
            global_config=self.test_root / 'global.json', runtime_dir=self.test_root / name,
            virtual_size=self.virtual_size, owner_id=f'trim-qualification-{name}',
            upper_mode=self.upper_mode)
        self._register_device(device)
        return device

    def mount(self, device, name):
        target = self.test_root / name
        target.mkdir()
        self._mount(device.device_path, target, '-o', 'noatime,nouuid,nodiscard')
        return target

    def seal(self, device, target, name):
        layer = self.test_root / f'{name}.commit'
        self._command('fsfreeze', '--freeze', str(target))
        try:
            self.client.restack_snapshot(device.device_id, layer)
        finally:
            self._command('fsfreeze', '--unfreeze', str(target))
        self._unmount(target)
        self._delete_device(device)
        return layer

    def export(self, name, layers):
        source = self.test_root / f'export-{name}.json'
        self._write_json(source, {'lowers': [{'file': str(p)} for p in layers],
                                 'upper': {}, 'repoBlobUrl': '', 'resultFile': ''})
        output = self.test_root / f'export-{name}.commit'
        with output.open('wb') as stream:
            result = consume_export_stream(lambda sock: self.client.export_compacted_image(
                source_image_config=source, global_config=self.test_root / 'global.json',
                stream_socket_path=sock), stream_socket_root=self.test_root,
                chunk_bytes=MIB, timeout_seconds=120, consume=stream.write)
        return output, result.size

    def verify(self, name, layers, expected):
        device = self.device(name, layers)
        target = self.mount(device, name+'-mount')
        assert not (target / 'deleted').exists(), 'deleted file reappeared'
        for filename, digest in expected.items():
            assert hashlib.sha256((target / filename).read_bytes()).hexdigest() == digest, filename
        self._unmount(target)
        self._delete_device(device)

    def qualify(self):
        self._start_daemon()
        device = self.device('initial', [])
        self._command('mkfs.xfs', '-f', '-K', str(device.device_path))
        target = self.mount(device, 'initial-mount')
        expected = {}
        for name, payload in [('keep', b'K'*8*MIB), ('deleted', b'D'*64*MIB),
                              ('zero', bytes(MIB))]:
            with (target / name).open('wb') as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            if name != 'deleted':
                expected[name] = hashlib.sha256(payload).hexdigest()
        # Sparse file includes a hole and a nonzero tail.
        with (target / 'hole').open('wb') as stream:
            stream.seek(4*MIB)
            stream.write(b'H'*4096)
            stream.flush()
            os.fsync(stream.fileno())
        expected['hole'] = hashlib.sha256((target / 'hole').read_bytes()).hexdigest()
        base = self.seal(device, target, 'base')
        results = {}
        for mode in ('baseline', 'trim'):
            device = self.device(mode, [base])
            target = self.mount(device, mode+'-mount')
            (target / 'deleted').unlink()
            self._command('sync', '-f', str(target))
            windows, foreground_latencies, foreground_errors = [], [], []
            stop, started_io = threading.Event(), threading.Event()
            # Exercise live metadata/data writes during FITRIM. The durable
            # checkpoint still happens only after this foreground writer stops.
            def foreground():
                try:
                    with (target / 'live').open('wb') as stream:
                        while not stop.is_set():
                            started = time.monotonic()
                            stream.seek(0)
                            stream.write(b'L'*4096)
                            stream.flush()
                            os.fsync(stream.fileno())
                            assert (target / 'keep').read_bytes() == b'K'*8*MIB
                            foreground_latencies.append(time.monotonic()-started)
                            started_io.set()
                            stop.wait(0.001)
                except BaseException as exc:
                    foreground_errors.append(exc)
                    started_io.set()
            writer = threading.Thread(target=foreground)
            writer.start()
            try:
                if not started_io.wait(10):
                    raise TimeoutError('foreground writer did not start')
                if mode == 'trim':
                    for offset in range(0, self.virtual_size, 64*MIB):
                        started = time.monotonic()
                        self._command('fstrim', '--offset', str(offset), '--length', str(64*MIB),
                                      '--minimum', str(MIB), str(target))
                        windows.append(time.monotonic()-started)
                else:
                    time.sleep(0.15)
            finally:
                stop.set()
                writer.join(30)
                if writer.is_alive():
                    raise TimeoutError('foreground writer did not stop')
            if foreground_errors:
                raise foreground_errors[0]
            expected['live'] = hashlib.sha256(b'L'*4096).hexdigest()
            delta = self.seal(device, target, mode+'-delta')
            suffix, suffix_size = self.export(mode+'-suffix', [delta])
            full, full_size = self.export(mode+'-full', [base, delta])
            self.verify(mode+'-partial-resume', [base, suffix], expected)
            self.verify(mode+'-full-resume', [full], expected)
            results[mode] = {'full_export_bytes': full_size, 'suffix_export_bytes': suffix_size,
                             'trim_window_seconds': windows, 'foreground_operation_seconds': foreground_latencies,
                             'remount_contents_verified': True}
        saved = results['baseline']['full_export_bytes'] - results['trim']['full_export_bytes']
        assert saved >= 48*MIB, f'trim did not eliminate deleted payload: {saved} bytes saved'
        return {'status': 'passed', 'kernel': os.uname().release,
                'backend_sha256': hashlib.sha256(self.daemon_binary.read_bytes()).hexdigest(),
                'saved_export_bytes': saved, 'results': results,
                'scope': 'Private XFS deletion/zero/hole/restack with concurrent fsync writes and reads; not sandbox wake load.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend-binary', required=True, type=Path)
    parser.add_argument('--work-root', default=Path('/var/tmp'), type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('root required for private device creation and mounts')
    if not Path('/dev/ublk-control').exists():
        parser.error('/dev/ublk-control unavailable; no devices or mounts created')
    for tool in ('mkfs.xfs', 'mount', 'umount', 'fsfreeze', 'fstrim', 'sync'):
        if shutil.which(tool) is None:
            parser.error(f'missing tool: {tool}')
    # Keep failure artifacts for investigation; never recursively delete a root
    # which could still contain a mount if cleanup encountered a device error.
    root = Path(tempfile.mkdtemp(prefix='xfs-trim-', dir=args.work_root)).resolve()
    qualifier = TrimQualifier(daemon_binary=args.backend_binary.resolve(), work_root=root,
        output=args.output, virtual_size=1024*MIB, upper_mode='sparse')
    qualifier.test_root = root
    try:
        result = qualifier.qualify()
    finally:
        qualifier._cleanup()
    result['artifact_root'] = str(root)
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
