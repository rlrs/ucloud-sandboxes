import ctypes
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from ucloud_sandboxes.direct_warden import CommandResult, DirectWardenError, SubprocessCommandRunner
from ucloud_sandboxes.image_rootfs import _mount_present
from ucloud_sandboxes.mount_status import _Statx, linux_mount_root


class MountStatusTests(unittest.TestCase):
    def test_linux_abi_buffer(self):
        self.assertEqual(ctypes.sizeof(_Statx), 256)
        self.assertEqual(_Statx.attributes.offset, 8)
        self.assertEqual(_Statx.attributes_mask.offset, 56)

    def test_requires_supported_attribute_and_fresh_successful_query(self):
        mounted = [True, False]

        def query(directory, path, flags, mask, output):
            self.assertEqual((directory, path, flags, mask), (-100, b"/image", 0x900, 0x7FF))
            output._obj.attributes_mask = 0x2000
            output._obj.attributes = 0x2000 if mounted.pop(0) else 0
            return 0

        with patch("ucloud_sandboxes.mount_status._statx_function", return_value=query):
            self.assertIs(linux_mount_root(Path('/image')), True)
            self.assertIs(linux_mount_root(Path('/image')), False)
        for query in (None, lambda *args: -1, lambda *args: 0):
            with patch("ucloud_sandboxes.mount_status._statx_function", return_value=query):
                self.assertIsNone(linux_mount_root(Path('/image')))

    def test_fast_path_does_not_spawn_and_failure_uses_existing_helper(self):
        runner = SubprocessCommandRunner()
        with patch("ucloud_sandboxes.image_rootfs.linux_mount_root", return_value=True), patch.object(runner, 'run') as run:
            self.assertTrue(_mount_present(Path('/image'), runner, 'mountpoint'))
            run.assert_not_called()
        with patch("ucloud_sandboxes.image_rootfs.linux_mount_root", return_value=None), patch.object(runner, 'run') as run:
            for status, expected in ((0, True), (1, False), (32, False)):
                run.return_value = CommandResult((), status, '', '')
                self.assertIs(_mount_present(Path('/image'), runner, 'mountpoint'), expected)
            run.return_value = CommandResult((), 2, '', 'permission denied')
            with self.assertRaisesRegex(DirectWardenError, 'permission denied'):
                _mount_present(Path('/image'), runner, 'mountpoint')

    def test_custom_helper_is_respected(self):
        runner = SubprocessCommandRunner()
        with patch("ucloud_sandboxes.image_rootfs.linux_mount_root") as query, patch.object(runner, 'run', return_value=CommandResult((), 32, '', '')) as run:
            self.assertFalse(_mount_present(Path('/image'), runner, '/custom/mountpoint'))
            query.assert_not_called()
            run.assert_called_once_with(('/custom/mountpoint', '--quiet', '/image'), timeout=60)

    @unittest.skipUnless(sys.platform == 'linux', 'native Linux statx')
    def test_native_root_and_regular_directory(self):
        self.assertIs(linux_mount_root(Path('/')), True)
        with tempfile.TemporaryDirectory() as directory:
            self.assertIs(linux_mount_root(Path(directory)), False)
            self.assertIsNone(linux_mount_root(Path(directory) / 'missing'))
