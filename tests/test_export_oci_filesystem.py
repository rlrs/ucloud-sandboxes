import gzip
import os
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
from export_oci_filesystem import bounded_output, export_tar, oci_manifest
from ucloud_sandboxes.oci_flat_delta import index_flat_tar


class FilesystemExportTests(unittest.TestCase):
    def test_bounded_scratch_is_unmounted_and_removed_after_export_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            backing = root / 'scratch.ext4'
            with patch('export_oci_filesystem.subprocess.run') as run, patch('os.path.ismount', return_value=True):
                with self.assertRaisesRegex(RuntimeError, 'failed export'):
                    with bounded_output(root / 'output', backing, 1024):
                        self.assertEqual(backing.stat().st_size, 1024)
                        raise RuntimeError('failed export')
                self.assertEqual(run.call_args.args[0], ['umount', str(root / 'output')])
            self.assertFalse(backing.exists())

    def test_unmount_failure_preserves_backing_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            backing = root / 'scratch.ext4'
            with patch('export_oci_filesystem.subprocess.run', side_effect=[None, None, RuntimeError('busy')]), \
                    patch('os.path.ismount', return_value=True):
                with self.assertRaisesRegex(RuntimeError, 'busy'):
                    with bounded_output(root / 'output', backing, 1024):
                        pass
            self.assertTrue(backing.exists())

    def test_export_preserves_links_metadata_and_xattrs_without_following_symlinks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'rootfs'
            source.mkdir()
            (source / 'file').write_bytes(b'payload')
            (source / 'file').chmod(0o640)
            os.link(source / 'file', source / 'linked')
            (source / 'symlink').symlink_to('/etc/passwd')
            (source / 'dev').mkdir()
            os.mkfifo(source / 'dev/fifo')
            os.setxattr(source / 'file', 'user.proof', b'preserved')
            dest = root / 'export.tar.gz'
            report = export_tar(source, dest)
            with gzip.open(dest, 'rb') as stream:
                entries = index_flat_tar(stream)
            self.assertNotIn('dev', entries)
            self.assertEqual(entries['file'].mode, 0o640)
            self.assertEqual(dict(entries['file'].pax)['SCHILY.xattr.user.proof'], 'preserved')
            self.assertEqual(entries['linked'].kind, tarfile.LNKTYPE)
            self.assertEqual(entries['linked'].linkname, 'file')
            self.assertEqual(entries['symlink'].linkname, '/etc/passwd')
            self.assertEqual(report['regular_file_bytes'], len(b'payload'))

    def test_manifest_normalization_preserves_blob_identities_and_rejects_unknown_encodings(self):
        layer = {'digest': 'sha256:' + 'a' * 64, 'size': 1,
                 'mediaType': 'application/vnd.docker.image.rootfs.diff.tar.gzip'}
        original = {'schemaVersion': 2, 'config': {'digest': 'sha256:' + 'b' * 64, 'size': 2}, 'layers': [layer]}
        result = oci_manifest(original)
        self.assertEqual(result['layers'][0]['digest'], layer['digest'])
        self.assertEqual(result['layers'][0]['mediaType'], 'application/vnd.oci.image.layer.v1.tar+gzip')
        self.assertEqual(original['layers'][0]['mediaType'], 'application/vnd.docker.image.rootfs.diff.tar.gzip')
        layer['mediaType'] = 'unknown'
        with self.assertRaises(ValueError):
            oci_manifest(original)
