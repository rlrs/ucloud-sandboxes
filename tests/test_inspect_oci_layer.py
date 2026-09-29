import argparse
import gzip
import hashlib
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest


spec = importlib.util.spec_from_file_location('inspect_oci_layer', Path(__file__).parents[1] / 'scripts/inspect_oci_layer.py')
inspector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inspector)


class LayerInspectionTests(unittest.TestCase):
    def fixture(self, root, names):
        payload = io.BytesIO()
        with tarfile.open(fileobj=payload, mode='w') as archive:
            for name, kind, content in names:
                member = tarfile.TarInfo(name)
                member.type, member.mode = kind, 0o755
                if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                    member.linkname = content
                elif kind == tarfile.REGTYPE:
                    member.size = len(content)
                archive.addfile(member, io.BytesIO(content) if kind == tarfile.REGTYPE else None)
        raw = payload.getvalue()
        compressed = gzip.compress(raw)
        path = root / 'blob'
        path.write_bytes(compressed)
        return argparse.Namespace(blob=path, compressed_bytes=len(compressed),
            compressed_digest='sha256:' + hashlib.sha256(compressed).hexdigest(),
            diff_id='sha256:' + hashlib.sha256(raw).hexdigest(), max_unpacked_bytes=1024**2,
            max_members=100, timeout_seconds=10)

    def test_full_digest_and_links_without_extraction(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = self.fixture(root, [('folder', tarfile.DIRTYPE, ''),
                ('folder/file', tarfile.REGTYPE, b'payload'),
                ('folder/hard', tarfile.LNKTYPE, 'folder/file'),
                ('folder/sym', tarfile.SYMTYPE, 'file')])
            result = inspector.inspect(args)
            self.assertTrue(result['diff_id_verified'])
            self.assertTrue(result['semantically_supported_by_current_extractor'])
            self.assertEqual(result['counts']['members'], 4)
            self.assertEqual(result['regular_payload_bytes'], 7)
            self.assertEqual(result['unpacked_bytes'], 10240)
            self.assertEqual(list(root.iterdir()), [args.blob])
            self.assertNotIn('folder', str(result))

    def test_whiteouts_missing_parent_and_cross_layer_link(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory), [('private/.wh.old', tarfile.REGTYPE, b''),
                ('private/.wh..wh..opq', tarfile.REGTYPE, b''),
                ('cross', tarfile.LNKTYPE, 'absent')])
            result = inspector.inspect(args)
            self.assertFalse(result['semantically_supported_by_current_extractor'])
            self.assertEqual(result['counts']['whiteouts'], 1)
            self.assertEqual(result['counts']['opaque_whiteouts'], 1)
            self.assertEqual(result['incompatibility_counts']['parent_requires_lower_context'], 2)
            self.assertEqual(result['incompatibility_counts']['hardlink_requires_lower_context'], 1)
            self.assertNotIn('private', str(result))

    def test_digest_and_resource_bounds_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.fixture(Path(directory), [('file', tarfile.REGTYPE, b'payload')])
            original = args.compressed_digest
            args.compressed_digest = 'sha256:' + '0' * 64
            with self.assertRaisesRegex(ValueError, 'Compressed identity'):
                inspector.inspect(args)
            args.compressed_digest = original
            args.max_unpacked_bytes = 100
            with self.assertRaisesRegex(inspector.InspectionLimit, 'byte_limit'):
                inspector.inspect(args)
            args.max_unpacked_bytes = 1024**2
            args.timeout_seconds = 0
            with self.assertRaisesRegex(inspector.InspectionLimit, 'timeout'):
                inspector.inspect(args)
