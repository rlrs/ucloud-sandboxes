from pathlib import Path
from tempfile import TemporaryDirectory
import os
import stat
import subprocess
import unittest
from ucloud_sandboxes.direct_service import sandbox_file_write_script


class AtomicShellFileWriteTests(unittest.TestCase):
    def run_write(self, path, body, *, umask='022', env=None):
        return subprocess.run(['/bin/sh', '-c', 'umask ' + umask + '; ' + sandbox_file_write_script(),
                               'ucloud-write', str(path)], input=body,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

    def test_binary_empty_overwrite_modes_and_quoted_parent(self):
        with TemporaryDirectory() as tmp:
            target = Path(tmp) / "space ' quoted" / '-file'
            for mask, body in [('022', b'a\x00b' * 32768), ('777', b''), ('000', b'replacement')]:
                with self.subTest(mask=mask):
                    result = self.run_write(target, body, umask=mask)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(target.read_bytes(), body)
                    self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
                    self.assertEqual(list(target.parent.glob('.ucloud-write.*')), [])

    def test_destination_symlink_is_replaced_and_directory_is_rejected(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            original = root / 'original'
            original.write_bytes(b'untouched')
            target = root / 'target'
            target.symlink_to(original)
            result = self.run_write(target, b'new')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(target.is_symlink())
            self.assertEqual(original.read_bytes(), b'untouched')
            result = self.run_write(root, b'bad')
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(list(root.glob('.ucloud-write.*')), [])

    def test_failed_copy_preserves_old_target_and_removes_temporary_file(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / 'target'
            target.write_bytes(b'old')
            tools = root / 'tools'
            tools.mkdir()
            cat = tools / 'cat'
            cat.write_text('#!/bin/sh\nprintf partial\nexit 4\n')
            cat.chmod(0o755)
            result = self.run_write(target, b'new', env={**os.environ, 'PATH': str(tools) + ':' + os.environ['PATH']})
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(target.read_bytes(), b'old')
            self.assertEqual(list(root.glob('.ucloud-write.*')), [])
