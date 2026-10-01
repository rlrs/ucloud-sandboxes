from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
from prepare_layered_task_pool import remove_export, resolve_as_user


class LayeredTaskPoolTests(unittest.TestCase):
    def test_export_cleanup_preserves_mounted_tree_and_does_not_follow_symlinks(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            export = root / 'export with spaces'
            export.mkdir()
            (export / 'archive').write_text('scratch')
            outside = root / 'persistent'
            outside.mkdir()
            (outside / 'receipt').write_text('keep')
            (export / 'link').symlink_to(outside, target_is_directory=True)
            mountinfo = root / 'mountinfo'
            mounted = str(export / 'jail/output').replace(' ', r'\040')
            mountinfo.write_text(f'1 2 3:4 / {mounted} rw - ext4 /dev/loop0 rw\n')
            with self.assertRaisesRegex(ValueError, 'mounted filesystem'):
                remove_export(export, mountinfo)
            self.assertTrue((export / 'archive').exists())
            mountinfo.write_text('1 2 3:4 / / rw - ext4 /dev/root rw\n')
            remove_export(export, mountinfo)
            self.assertFalse(export.exists())
            self.assertEqual((outside / 'receipt').read_text(), 'keep')
            export.symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, 'symlink'):
                remove_export(export, mountinfo)

    def test_source_resolution_restores_identity_even_on_quota_failure(self):
        with patch('prepare_layered_task_pool.os.geteuid', return_value=0), \
             patch('prepare_layered_task_pool.os.getegid', return_value=0), \
             patch('prepare_layered_task_pool.os.seteuid') as uid, \
             patch('prepare_layered_task_pool.os.setegid') as gid:
            def resolve(source):
                self.assertEqual(uid.call_args.args, (999,))
                raise RuntimeError('quota')
            with self.assertRaisesRegex(RuntimeError, 'quota'):
                resolve_as_user(resolve, 'source', 999, 983)
            self.assertEqual(uid.call_args.args, (0,))
            self.assertEqual(gid.call_args.args, (0,))
