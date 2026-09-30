import contextlib
import gzip
import hashlib
import io
from pathlib import Path
import tempfile
import tarfile
import unittest
from unittest.mock import patch

from ucloud_sandboxes.oci_flat_delta import (
    UnsupportedFlatImage, index_flat_tar, plan_flat_delta, write_flat_delta,
)


def archive(entries):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode='w', format=tarfile.PAX_FORMAT) as tar:
        for path, kind, value, mode in entries:
            info = tarfile.TarInfo(path)
            info.type, info.mode, info.mtime = kind, mode, 1234
            info.uid, info.gid = 12, 34
            if kind == tarfile.REGTYPE:
                payload = value.encode()
                info.size = len(payload)
                tar.addfile(info, io.BytesIO(payload))
            else:
                if kind in {tarfile.LNKTYPE, tarfile.SYMTYPE}:
                    info.linkname = value
                tar.addfile(info)
    return output.getvalue()


ROOT = [('.', tarfile.DIRTYPE, '', 0o755), ('app', tarfile.DIRTYPE, '', 0o755)]


class FlatDeltaTests(unittest.TestCase):
    def test_delta_retains_common_files_and_encodes_deletion_and_mode_changes(self):
        base_tar = archive(ROOT + [('app/common', tarfile.REGTYPE, 'large common dependency', 0o644),
            ('app/gone', tarfile.REGTYPE, 'obsolete', 0o644), ('app/tool', tarfile.REGTYPE, 'same', 0o644)])
        target_tar = archive(ROOT + [('app/common', tarfile.REGTYPE, 'large common dependency', 0o644),
            ('app/new', tarfile.REGTYPE, 'small', 0o644), ('app/tool', tarfile.REGTYPE, 'same', 0o755)])
        base, target = map(lambda value: index_flat_tar(io.BytesIO(value)), (base_tar, target_tar))
        plan = plan_flat_delta(base, target)
        self.assertEqual(plan.changed, {'app/new', 'app/tool'})
        self.assertEqual(plan.removed, ('app/gone',))
        self.assertEqual(plan.regular_file_bytes, 9)
        output = io.BytesIO()
        write_flat_delta(io.BytesIO(target_tar), output, target, plan)
        with tarfile.open(fileobj=io.BytesIO(output.getvalue())) as delta:
            names = delta.getnames()
            self.assertNotIn('app/common', names)
            self.assertIn('app/.wh.gone', names)
            self.assertEqual(delta.extractfile('app/new').read(), b'small')
            self.assertEqual(delta.getmember('app/tool').mode, 0o755)
            self.assertEqual(len(names), len(set(names)))
            self.assertEqual(delta.getmember('app').mtime, 1234)
            self.assertEqual(delta.getmember('.').mtime, 1234)

    def test_hardlink_closure_is_recreated_when_contents_or_membership_change(self):
        base_tar = archive(ROOT + [('app/a', tarfile.REGTYPE, 'old', 0o644),
                                  ('app/b', tarfile.LNKTYPE, 'app/a', 0o644)])
        base = index_flat_tar(io.BytesIO(base_tar))
        for entries in ([('app/a', tarfile.REGTYPE, 'new', 0o644), ('app/b', tarfile.LNKTYPE, 'app/a', 0o644)],
                        [('app/a', tarfile.REGTYPE, 'old', 0o644)]):
            with self.subTest(entries=entries):
                target_tar = archive(ROOT + entries)
                target = index_flat_tar(io.BytesIO(target_tar))
                plan = plan_flat_delta(base, target)
                self.assertEqual(plan.changed, {e[0] for e in entries})
                output = io.BytesIO()
                write_flat_delta(io.BytesIO(target_tar), output, target, plan)
                with tarfile.open(fileobj=io.BytesIO(output.getvalue())) as delta:
                    self.assertIn('app/a', delta.getnames())
                    if len(entries) == 2:
                        self.assertTrue(delta.getmember('app/b').islnk())
                    else:
                        self.assertIn('app/.wh.b', delta.getnames())

    def test_type_replacement_whiteouts_cover_descendants(self):
        base = index_flat_tar(io.BytesIO(archive(ROOT + [('app/dir', tarfile.DIRTYPE, '', 0o755),
            ('app/dir/file', tarfile.REGTYPE, 'old', 0o644)])))
        target = index_flat_tar(io.BytesIO(archive(ROOT + [('app/dir', tarfile.SYMTYPE, '/other', 0o777)])))
        plan = plan_flat_delta(base, target)
        self.assertEqual(plan.removed, ('app/dir',))
        self.assertEqual(plan.changed, {'app/dir'})

    def test_malformed_or_unsupported_flat_layouts_fail_closed(self):
        cases = [ROOT + [('app/../escape', tarfile.REGTYPE, 'x', 0o644)],
                 ROOT + [('app/.wh.deleted', tarfile.REGTYPE, '', 0o644)],
                 ROOT + [('app/a', tarfile.REGTYPE, '1', 0o644), ('app/a', tarfile.REGTYPE, '2', 0o644)],
                 ROOT + [('app/a', tarfile.LNKTYPE, 'app/missing', 0o644)],
                 ROOT + [('app/link', tarfile.SYMTYPE, '/tmp', 0o777),
                         ('app/link/file', tarfile.REGTYPE, 'x', 0o644)],
                 ROOT + [('app/socket', tarfile.FIFOTYPE, '', 0o644)]]
        for entries in cases:
            with self.subTest(entries=entries), self.assertRaises(UnsupportedFlatImage):
                index_flat_tar(io.BytesIO(archive(entries)))

    def test_changed_stream_is_rejected_even_for_a_file_omitted_from_the_delta(self):
        old = archive(ROOT + [('app/common', tarfile.REGTYPE, 'before', 0o644)])
        index = index_flat_tar(io.BytesIO(old))
        plan = plan_flat_delta(index, index)
        changed = archive(ROOT + [('app/common', tarfile.REGTYPE, 'after!', 0o644)])
        with self.assertRaisesRegex(UnsupportedFlatImage, 'contents changed'):
            write_flat_delta(io.BytesIO(changed), io.BytesIO(), index, plan)


class FlatDeltaCommandTests(unittest.TestCase):
    def test_digest_pinned_command_is_reproducible_and_rejects_changed_inputs(self):
        from scripts.plan_flat_image_delta import main
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            blobs = [gzip.compress(archive(ROOT + [('app/a', tarfile.REGTYPE, body, 0o644)]), mtime=0)
                     for body in ('old', 'new')]
            for name, blob in zip(('base', 'target'), blobs):
                (root / name).write_bytes(blob)
            args = ['delta', '--base-layer', str(root / 'base'), '--base-digest',
                    'sha256:' + hashlib.sha256(blobs[0]).hexdigest(),
                    '--target-layer', str(root / 'target'), '--target-digest',
                    'sha256:' + hashlib.sha256(blobs[1]).hexdigest()]
            for output in ('first', 'second'):
                with patch('sys.argv', args + ['--output', str(root / output)]), contextlib.redirect_stdout(io.StringIO()):
                    main()
            self.assertEqual((root / 'first/delta.tar.gz').read_bytes(), (root / 'second/delta.tar.gz').read_bytes())
            self.assertEqual((root / 'first/report.json').read_bytes(), (root / 'second/report.json').read_bytes())
            (root / 'target').write_bytes(blobs[0])
            with patch('sys.argv', args + ['--output', str(root / 'bad')]), self.assertRaisesRegex(ValueError, 'digest mismatch'):
                main()
            self.assertFalse((root / 'bad').exists())


class SourceFilesystemQualificationTests(unittest.TestCase):
    def test_source_model_includes_link_identity_and_rejects_mismatches(self):
        from copy import deepcopy
        from ucloud_sandboxes.flat_image_qualification import (
            RUNTIME_FILES, RUNTIME_TREES, SCAN_OUTPUT, compare_snapshot, expected_filesystem,
        )
        data = archive(ROOT + [('app/a', tarfile.REGTYPE, 'payload', 0o640),
                               ('app/b', tarfile.LNKTYPE, 'app/a', 0o640),
                               ('app/link', tarfile.SYMTYPE, 'a', 0o777)])
        index = index_flat_tar(io.BytesIO(data))
        expected = expected_filesystem(index)
        self.assertEqual(expected['/app/b']['sha256'], hashlib.sha256(b'payload').hexdigest())
        self.assertEqual(expected['/app/b']['nlink'], 2)
        self.assertEqual(expected['/app/b']['hardlinks'], ['/app/a', '/app/b'])
        self.assertEqual(expected['/app/a']['mtime_ns'], 0)
        snapshot = {'entries': expected, 'errors': [],
                    'excluded': sorted(RUNTIME_TREES | (RUNTIME_FILES - {'/.ucloud-init'}) | {SCAN_OUTPUT})}
        formats = [{'layout': 1, 'excludes': ['dev', 'proc', 'sys', 'run']}]
        self.assertTrue(compare_snapshot(index, snapshot, layer_formats=formats)['equivalent'])
        for field, value in [('sha256', 'bad'), ('mode', 0o777), ('nlink', 1), ('mtime_ns', 123)]:
            with self.subTest(field=field):
                changed = deepcopy(snapshot)
                changed['entries']['/app/a'][field] = value
                self.assertFalse(compare_snapshot(index, changed, layer_formats=formats)['equivalent'])
        for changed in (dict(snapshot, errors=['unreadable']), dict(snapshot, excluded=[])):
            with self.assertRaises(ValueError):
                compare_snapshot(index, changed, layer_formats=formats)
        with self.assertRaises(ValueError):
            compare_snapshot(index, snapshot, layer_formats=[{'layout': 2}])

    def test_scan_certificate_requires_identical_bytes_ranges_model_and_runtime(self):
        from copy import deepcopy
        from types import SimpleNamespace
        from ucloud_sandboxes.flat_image_qualification import qualification_key
        index = index_flat_tar(io.BytesIO(archive(ROOT + [('app/a', tarfile.REGTYPE, 'payload', 0o644)])))
        profile = {'source_layers': ['old-layer'], 'parent': 'old-parent', 'image_digest': 'image',
                   'image_size': 4096, 'chunks': [{'digest': 'range', 'offset': 0, 'length': 4096}],
                   'format': {'layout': 1}, 'producer_key': 'trusted'}
        def component(value):
            return SimpleNamespace(unsigned=lambda: deepcopy(value))
        config = {'Env': ['MODE=safe']}
        key = qualification_key(index, [component(profile)], config, 'worker-v1')
        same_bytes = {**profile, 'source_layers': ['new-layer'], 'parent': 'new-parent'}
        self.assertEqual(key, qualification_key(index, [component(same_bytes)], config, 'worker-v1'))
        different_range = {**profile, 'chunks': [{'digest': 'different', 'offset': 0, 'length': 4096}]}
        self.assertNotEqual(key, qualification_key(index, [component(different_range)], config, 'worker-v1'))
        self.assertNotEqual(key, qualification_key(index, [component(profile)], {'Env': ['MODE=changed']}, 'worker-v1'))
        self.assertNotEqual(key, qualification_key(index, [component(profile)], config, 'worker-v2'))
        changed = index_flat_tar(io.BytesIO(archive(ROOT + [('app/a', tarfile.REGTYPE, 'changed', 0o644)])))
        self.assertNotEqual(key, qualification_key(changed, [component(profile)], config, 'worker-v1'))

    def test_runtime_tmpfs_contents_are_excluded_but_app_files_are_not(self):
        from ucloud_sandboxes.flat_image_qualification import expected_filesystem
        data = archive(ROOT + [('tmp', tarfile.DIRTYPE, '', 0o1777),
                               ('tmp/installer.tar.gz', tarfile.REGTYPE, 'runtime hides this', 0o644),
                               ('app/required', tarfile.REGTYPE, 'runtime retains this', 0o644)])
        expected = expected_filesystem(index_flat_tar(io.BytesIO(data)))
        self.assertNotIn('/tmp', expected)
        self.assertNotIn('/tmp/installer.tar.gz', expected)
        self.assertEqual(expected['/app/required']['sha256'], hashlib.sha256(b'runtime retains this').hexdigest())

    def test_only_an_absent_workspace_gets_the_exact_runtime_created_directory(self):
        from copy import deepcopy
        import stat
        from ucloud_sandboxes.flat_image_qualification import (
            RUNTIME_FILES, RUNTIME_TREES, SCAN_OUTPUT, compare_snapshot, expected_filesystem,
        )
        index = index_flat_tar(io.BytesIO(archive(ROOT)))
        expected = expected_filesystem(index)
        self.assertEqual(expected['/workspace']['mode'], stat.S_IFDIR | 0o1777)
        snapshot = {'entries': deepcopy(expected), 'errors': [],
                    'excluded': sorted(RUNTIME_TREES | (RUNTIME_FILES - {'/.ucloud-init'}) | {SCAN_OUTPUT})}
        snapshot['entries']['/workspace']['mtime_ns'] = 123
        formats = [{'layout': 1, 'excludes': ['dev', 'proc', 'sys', 'run']}]
        self.assertTrue(compare_snapshot(index, snapshot, layer_formats=formats)['equivalent'])
        snapshot['entries']['/workspace']['uid'] = 1000
        self.assertFalse(compare_snapshot(index, snapshot, layer_formats=formats)['equivalent'])
        present = index_flat_tar(io.BytesIO(archive(ROOT + [
            ('workspace', tarfile.DIRTYPE, '', 0o750), ('workspace/input', tarfile.REGTYPE, 'required', 0o640)])))
        snapshot['entries'] = expected_filesystem(present)
        self.assertEqual(snapshot['entries']['/workspace']['mode'], stat.S_IFDIR | 0o750)
        snapshot['entries']['/workspace']['mtime_ns'] = 123
        self.assertFalse(compare_snapshot(present, snapshot, layer_formats=formats)['equivalent'])
        snapshot['entries'] = expected_filesystem(present)
        snapshot['entries'].pop('/workspace/input')
        self.assertFalse(compare_snapshot(present, snapshot, layer_formats=formats)['equivalent'])

    def test_scanner_output_cannot_hide_a_source_file(self):
        from ucloud_sandboxes.flat_image_qualification import expected_filesystem
        data = archive([('.', tarfile.DIRTYPE, '', 0o755), ('tmp', tarfile.DIRTYPE, '', 0o1777),
                        ('tmp/ucloud-filesystem-proof.json.gz', tarfile.REGTYPE, 'source data', 0o644)])
        with self.assertRaisesRegex(ValueError, 'qualification output'):
            expected_filesystem(index_flat_tar(io.BytesIO(data)))


if __name__ == '__main__':
    unittest.main()
