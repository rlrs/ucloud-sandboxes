import io
import json
from pathlib import Path
import sys
import tarfile
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
from prepare_shared_task_image import write_flat_index
from shared_dependency_index import build_index, qualified_candidates, rank_anchors
from tests.test_oci_flat_delta import ROOT, archive
from ucloud_sandboxes.oci_flat_delta import index_flat_tar


class SharedDependencyIndexTests(unittest.TestCase):
    def test_index_rebuild_excludes_incomplete_or_unqualified_sources(self):
        import hashlib
        from tests.test_source_receipts import SourceReceiptTests
        with TemporaryDirectory() as raw:
            root = Path(raw)
            source = 'example/source:latest'
            work = root/'work'/hashlib.sha256(source.encode()).hexdigest()
            work.mkdir(parents=True)
            resolved = SourceReceiptTests().receipt()
            (work/'resolved.json').write_text(json.dumps(resolved))
            row = {'status': 'ready', 'method': 'verified-flat-delta-v1',
                   'qualification': {'equivalent': True, 'mode': 'full_source_scan'},
                   'components': [{'bytes': 100}], 'source_reference': resolved['reference'],
                   'reference': 'private/prepared@sha256:'+'f'*64}
            def catalog(value):
                (root/'catalog.json').write_text(json.dumps({'images': {source: value}}))
            catalog(row)
            self.assertEqual(qualified_candidates([root]), [])
            (work/'source-index.json.gz').touch()
            self.assertEqual(qualified_candidates([root])[0]['source'], source)
            catalog({**row, 'qualification': {'equivalent': True, 'mode': 'certificate'}})
            self.assertEqual(qualified_candidates([root]), [])
            catalog({**row, 'source_reference': 'wrong'})
            with self.assertRaisesRegex(ValueError, 'source mismatch'):
                qualified_candidates([root])

    def test_selects_exact_dependencies_across_projects_and_accounts_for_modes_and_small_files(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            common = ('app/library', tarfile.REGTYPE, 'large dependency' * 5000, 0o644)
            task = ('app/task', tarfile.REGTYPE, 'small task', 0o644)
            versions = [ROOT, ROOT + [common], ROOT + [(*common[:3], 0o755)]]
            candidates = []
            layer = {'digest': 'sha256:' + 'a' * 64, 'size': 123}
            for number, entries in enumerate(versions):
                path = root / f'{number}.json.gz'
                write_flat_index(path, layer, index_flat_tar(io.BytesIO(archive(entries))))
                candidates.append({'source': f'project-{number}', 'index_path': str(path), 'layer': layer, 'erofs_bytes': 100})
            db = root / 'index.sqlite'
            result = build_index(db, candidates)
            self.assertEqual(result['anchors'], 3)
            target = index_flat_tar(io.BytesIO(archive(ROOT + [common, task])))
            ranked = rank_anchors(db, target)
            self.assertEqual(ranked[0]['source'], 'project-1')
            self.assertEqual(ranked[0]['changed_regular_file_bytes'], len('small task'))
            self.assertGreater(ranked[-1]['changed_regular_file_bytes'], 75000)
            self.assertNotIn('project-1', [r['source'] for r in rank_anchors(db, target, excluded_source='project-1')])
            with self.assertRaisesRegex(ValueError, 'immutable'):
                build_index(db, candidates)

    def test_full_scoring_recreates_hardlinks_even_when_large_file_matches(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            common = ('app/library', tarfile.REGTYPE, 'a' * 20000, 0o644)
            anchor = index_flat_tar(io.BytesIO(archive(ROOT + [common, ('app/link', tarfile.LNKTYPE, 'app/library', 0o644)])))
            target = index_flat_tar(io.BytesIO(archive(ROOT + [common])))
            layer = {'digest': 'sha256:' + 'b' * 64, 'size': 456}
            path = root / 'anchor.json.gz'
            write_flat_index(path, layer, anchor)
            db = root / 'index.sqlite'
            build_index(db, [{'source': 'anchor', 'index_path': str(path), 'layer': layer, 'erofs_bytes': 20_000}])
            result = rank_anchors(db, target)[0]
            self.assertEqual(result['changed_regular_file_bytes'], 20_000)
            self.assertEqual(result['removed_paths'], 1)


class DependencySelectionResumeTests(unittest.TestCase):
    def test_resume_uses_authenticated_saved_source_without_resolving_mutable_tag(self):
        from types import SimpleNamespace
        from select_shared_dependencies import dependency_selection, request_identity
        from tests.test_source_receipts import SourceReceiptTests
        from cache_source_receipts import validated
        with TemporaryDirectory() as raw:
            root = Path(raw)
            source = 'example/source:latest'
            args = SimpleNamespace(root=root, source=source, anchor='private/base@sha256:'+'a'*64,
                pinned_source=None, compression_level=6, anchor_filesystem_source=None)
            resolved = validated(source, SourceReceiptTests().receipt())
            receipt = {'schema': 1, 'request': request_identity(args), 'source_reference': resolved['reference'],
                'chosen': {'reference': 'private/selected@sha256:'+'b'*64, 'original': True}}
            (root/'dependency-selection.json').write_text(json.dumps(receipt))
            (root/'selection-source.json').write_text(json.dumps(resolved))
            with patch('select_shared_dependencies.SourceResolver') as resolver:
                with dependency_selection(args):
                    self.assertEqual(args._dependency_source_metadata, resolved)
                    self.assertEqual(args.anchor, receipt['chosen']['reference'])
                resolver.assert_not_called()
            resolved['config_json'] += ' '
            (root/'selection-source.json').write_text(json.dumps(resolved))
            with self.assertRaisesRegex(ValueError, 'config identity'):
                with dependency_selection(args):
                    self.fail('modified metadata accepted')

    def test_resume_keeps_chosen_anchor_and_rejects_a_different_request(self):
        from types import SimpleNamespace
        from select_shared_dependencies import request_identity, restore_selection
        args = SimpleNamespace(source='docker.io/example/task:one', anchor='private/base@sha256:' + 'a'*64,
                               pinned_source=None, compression_level=6, anchor_filesystem_source=None)
        selected = {'source': 'another-project', 'reference': 'private/selected@sha256:' + 'b'*64,
                    'source_reference': 'docker.io/example/another@sha256:' + 'c'*64}
        receipt = {'schema': 1, 'request': request_identity(args), 'chosen': selected}
        restore_selection(args, receipt)
        self.assertEqual(args.anchor, selected['reference'])
        self.assertEqual(args.anchor_filesystem_source, selected['source_reference'])
        with self.assertRaisesRegex(ValueError, 'another request'):
            restore_selection(args, receipt)

    def test_existing_preparation_is_not_reassigned_when_policy_is_enabled(self):
        from types import SimpleNamespace
        from select_shared_dependencies import dependency_selection
        with TemporaryDirectory() as raw:
            root = Path(raw)
            (root / 'identity.json').write_text('{}')
            args = SimpleNamespace(root=root, source='docker.io/example/task:one',
                anchor='private/base@sha256:'+'a'*64, pinned_source=None, compression_level=9,
                anchor_filesystem_source=None, dependency_index=root/'does-not-exist.sqlite')
            with dependency_selection(args):
                self.assertEqual(args.anchor, 'private/base@sha256:'+'a'*64)
                self.assertFalse((root/'dependency-selection.json').exists())
