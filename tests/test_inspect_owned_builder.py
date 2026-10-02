import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import inspect_owned_builder as inspect
from scripts import qualify_builder_slots as qualify
from scripts import upgrade_owned_builder as helper

TEST_TIER = "contract"


class InstalledRuntimeTests(unittest.TestCase):
    def test_full_file_set_and_bytes_required_ignoring_only_bytecode(self):
        with TemporaryDirectory() as temporary:
            source = Path(temporary)
            package = source / 'ucloud_sandboxes'
            package.mkdir()
            (package / 'images.py').write_bytes(b'original')
            members = {'ucloud_sandboxes/images.py': b'original', 'dist-info/METADATA': b'ignored'}
            (package / '__pycache__').mkdir()
            (package / '__pycache__' / 'images.pyc').write_bytes(b'bytecode')
            self.assertEqual(inspect.audit_package(source, members), 1)
            (package / 'extra.py').write_bytes(b'extra')
            with self.assertRaisesRegex(ValueError, 'file set'):
                inspect.audit_package(source, members)
            (package / 'extra.py').unlink()
            (package / 'images.py').write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'bytes'):
                inspect.audit_package(source, members)
            (package / 'images.py').unlink()
            (package / 'images.py').symlink_to('/unused')
            with self.assertRaisesRegex(ValueError, 'symlink'):
                inspect.audit_package(source, members)

    def test_actual_service_cwd_shadow_is_rejected(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, cwd = root / 'site-packages', root / 'working'
            source.mkdir()
            cwd.mkdir()
            for location in (source, cwd):
                package = location / 'ucloud_sandboxes'
                package.mkdir()
                (package / '__init__.py').write_text('')
                (package / 'cli.py').write_text('')
            child = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(10)'], cwd=cwd)
            try:
                state = dict(pid=child.pid, source=source, argv=[sys.executable],
                             env={**os.environ, 'PYTHONPATH': str(source)})
                with self.assertRaisesRegex(ValueError, 'import origin'):
                    inspect.verify_import_origin(state)
                (cwd / 'ucloud_sandboxes' / 'cli.py').unlink()
                (cwd / 'ucloud_sandboxes' / '__init__.py').unlink()
                (cwd / 'ucloud_sandboxes').rmdir()
                self.assertEqual(inspect.verify_import_origin(state), cwd)
            finally:
                child.terminate()
                child.wait(timeout=2)

    def test_inspection_is_not_an_upgrade_and_rejects_process_change(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            wheel = root / 'wheel'
            wheel.write_bytes(b'owned')
            source = root / 'site-packages'
            (source / 'ucloud_sandboxes').mkdir(parents=True)
            (source / 'ucloud_sandboxes' / 'images.py').write_bytes(b'original')
            state = dict(source=source, pid=100, argv=['python', '--max-finishing-image-builds', '2'],
                         node_epoch='epoch', job_id='101')
            args = SimpleNamespace(wheel=wheel, wheel_sha256=inspect.sha(wheel),
                                   expected_job_id='101', expected_node_epoch=None)
            heartbeat = dict(draining=False, admission_open=True, labels={helper.LABEL: '4'})
            with patch.object(helper, 'discover', return_value=state) as discover, \
                    patch.object(helper, 'heartbeat', return_value=heartbeat), \
                    patch.object(helper, 'wheel_members', return_value={'ucloud_sandboxes/images.py': b'original'}), \
                    patch.object(inspect, 'verify_import_origin', return_value=root):
                receipt = inspect.inspect(args, helper)
                self.assertFalse(receipt['service_changed'])
                self.assertTrue(receipt['complete'])
                self.assertEqual(receipt['installed_files_match_wheel'], 1)
                self.assertNotIn('argv', str(receipt))
                discover.side_effect = [state, {**state, 'pid': 101}]
                with self.assertRaisesRegex(ValueError, 'process changed'):
                    inspect.inspect(args, helper)

    def test_harness_requires_truthful_pinned_complete_fresh_receipts(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            nodes = ['101', '102', '103', '104']
            paths = [root / (node + '.json') for node in nodes]
            wheel = 'a' * 64
            values = [dict(kind='installed_runtime', complete=True, service_changed=False,
                           wheel_sha256=wheel, inspector_sha256=inspect.sha(inspect.__file__),
                           installed_files_match_wheel=165, runtime_file_set_matches=True,
                           import_origins_match=True,
                           after=dict(job_id=node, node_epoch='epoch-' + node, finishing_capacity=2,
                                      active_builds=0, admission_open=True, draining=False)) for node in nodes]
            for path, value in zip(paths, values):
                path.write_text(json.dumps(value))
            rows = qualify.verify_builder_receipts(paths, nodes, 6, wheel)
            self.assertTrue(all(row['verification'] == 'installed_runtime' for row in rows))
            for pin in (None, 'b' * 64):
                with self.assertRaises(ValueError):
                    qualify.verify_builder_receipts(paths, nodes, 6, pin)
            for key, value in [('service_changed', True), ('installed_files_match_wheel', 0),
                               ('runtime_file_set_matches', False), ('import_origins_match', False),
                               ('inspector_sha256', 'b' * 64)]:
                with self.subTest(key=key):
                    paths[0].write_text(json.dumps({**values[0], key: value}))
                    with self.assertRaises(ValueError):
                        qualify.verify_builder_receipts(paths, nodes, 6, wheel)


if __name__ == '__main__':
    unittest.main()
