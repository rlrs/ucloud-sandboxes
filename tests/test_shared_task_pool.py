import contextlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
import prepare_shared_task_pool as pool


class SharedTaskPoolTests(unittest.TestCase):
    def run_pool(self, root, growth, run, extra=(), health=None):
        config = root / 'config.json'
        config.write_text('{}')
        args = ['pool', '--root', str(root), '--config', str(config), '--gateway', 'https://example.invalid',
                '--sdk-wheel', str(root / 'sdk.whl'), '--workers', '2', '--growth-limit-gib', str(growth), *extra]
        disk = SimpleNamespace(used_bytes=100, available_bytes=600 * 1024**3)
        with patch('sys.argv', args), patch('ucloud_sandboxes.config.DeploymentConfig.from_dict',
                   return_value=SimpleNamespace(control_state_file=lambda: root / 'state.sqlite', registry_url='http://registry')), \
             patch.object(pool.RegistryHealthGate, 'ready', return_value=True, side_effect=health), \
             patch('ucloud_sandboxes.registry_disk.registry_disk_usage', return_value=disk), \
             patch.object(pool.subprocess, 'run', side_effect=run) as invoked, contextlib.redirect_stdout(io.StringIO()):
            pool.main()
        return json.loads((root / 'catalog.json').read_text()), invoked

    def test_storage_admission_defers_before_starting_a_child(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'plan.json').write_text(json.dumps({'schema': 1, 'images': [{'source': 'task', 'anchor': 'base'}]}))
            catalog, invoked = self.run_pool(root, 1, lambda *args, **kwargs: self.fail('unexpected child'))
            invoked.assert_not_called()
            self.assertEqual(catalog['images']['task']['status'], 'deferred')
            self.assertIn('storage budget', catalog['images']['task']['error'])
            progress = json.loads((root / 'progress.json').read_text())
            self.assertEqual(progress['admission_block'], 'batch storage budget')
            self.assertEqual(progress['queued'], 0)

    def test_child_success_requires_an_explicit_equivalence_receipt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            images = [{'source': source, 'anchor': 'base'} for source in ('good', 'bad')]
            (root / 'plan.json').write_text(json.dumps({'schema': 1, 'images': images}))
            def run(command, **kwargs):
                source = command[command.index('--source') + 1]
                work = Path(command[command.index('--root') + 1])
                work.mkdir(parents=True)
                row = {'source': source, 'status': 'ready', 'qualification': {'equivalent': source == 'good'}}
                (work / 'catalog.json').write_text(json.dumps({'images': {source: row}}))
            return SimpleNamespace(returncode=0)
            catalog, invoked = self.run_pool(root, 16, run)
            self.assertEqual(invoked.call_count, 2)
            self.assertEqual(catalog['images']['good']['status'], 'ready')
            self.assertEqual(catalog['images']['bad']['status'], 'failed')

    def test_registry_outage_pauses_admission_without_failing_candidates(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'plan.json').write_text(json.dumps({'schema': 1, 'images': [{'source': 'task', 'anchor': 'base'}]}))
            def run(command, **kwargs):
                work = Path(command[command.index('--root') + 1])
                work.mkdir(parents=True)
                (work / 'catalog.json').write_text(json.dumps({'images': {
                    'task': {'source': 'task', 'status': 'ready', 'qualification': {'equivalent': True}}}}))
                return SimpleNamespace(returncode=0)
            with patch.object(pool.time, 'sleep') as sleep:
                catalog, invoked = self.run_pool(root, 16, run, health=[False, True])
            sleep.assert_called_once_with(5)
            invoked.assert_called_once()
            self.assertEqual(catalog['images']['task']['status'], 'ready')
            progress = json.loads((root / 'progress.json').read_text())
            self.assertEqual(progress['active'], 0)
            self.assertIsNone(progress['admission_block'])

    def test_resume_reevaluates_storage_deferrals_without_resetting_budget(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'plan.json').write_text(json.dumps({'schema': 1, 'images': [{'source': 'task', 'anchor': 'base'}]}))
            self.run_pool(root, 1, lambda *a, **kw: self.fail('unexpected child'))
            original_budget = (root / 'budget.json').read_bytes()
            def run(command, **kwargs):
                progress = json.loads((root / 'progress.json').read_text())
                self.assertEqual(progress['counts']['deferred'], 1)
                self.assertEqual(progress['queued'], 1)
                self.assertIsNone(progress['admission_block'])
                work = Path(command[command.index('--root') + 1])
                work.mkdir(parents=True)
                (work / 'catalog.json').write_text(json.dumps({'images': {'task': {
                    'source': 'task', 'status': 'ready', 'qualification': {'equivalent': True}}}}))
                return SimpleNamespace(returncode=0)
            catalog, invoked = self.run_pool(root, 16, run)
            self.assertEqual(invoked.call_count, 1)
            self.assertEqual(catalog['images']['task']['status'], 'ready')
            self.assertEqual((root / 'budget.json').read_bytes(), original_budget)
            self.assertEqual(len(list((root / 'attempts').glob('*.json'))), 1)
            progress = json.loads((root / 'progress.json').read_text())
            self.assertEqual(progress['queued'], 0)
            self.assertEqual(progress['active'], 0)
            self.assertEqual(progress['completed_current_run'], 1)

    def test_public_cooldown_pauses_admission_without_starting_waiting_children(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = 'docker.io/example/task:one'
            (root / 'plan.json').write_text(json.dumps({'schema': 1, 'images': [{'source': source, 'anchor': 'base'}]}))
            gate = root / 'image-pool-locks'
            gate.mkdir()
            (gate / 'source-docker.io.json').write_text(json.dumps({'next_request_at': 1060, 'failures': 1}))
            now, sleeps, starts = [1000.0], [], []
            def sleep(seconds):
                sleeps.append(seconds)
                now[0] += seconds
            def run(command, **kwargs):
                starts.append(now[0])
                work = Path(command[command.index('--root') + 1])
                work.mkdir(parents=True)
                row = {'source': source, 'status': 'ready', 'qualification': {'equivalent': True}}
                (work / 'catalog.json').write_text(json.dumps({'images': {source: row}}))
                return SimpleNamespace(returncode=0)
            with patch.object(pool.time, 'time', side_effect=lambda: now[0]), patch.object(pool.time, 'sleep', side_effect=sleep):
                catalog, _ = self.run_pool(root, 16, run)
            self.assertEqual(sleeps, [30, 30])
            self.assertEqual(starts, [1060])
            self.assertEqual(catalog['images'][source]['status'], 'ready')

    def test_retry_preserves_failed_journal_and_original_budget_baseline(self):
        import hashlib
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'plan.json').write_text(json.dumps({'schema': 1, 'images': [{'source': 'task', 'anchor': 'base'}]}))
            (root / 'results').mkdir()
            key = hashlib.sha256(b'task').hexdigest()
            old = {'source': 'task', 'anchor': 'base', 'status': 'failed', 'error': 'old verifier'}
            (root / 'results' / (key + '.json')).write_text(json.dumps(old))
            (root / 'budget.json').write_text(json.dumps({'initial_used_bytes': 23}))
            def run(command, **kwargs):
                work = Path(command[command.index('--root') + 1])
                work.mkdir(parents=True)
                row = {'source': 'task', 'status': 'ready', 'qualification': {'equivalent': True}}
                (work / 'catalog.json').write_text(json.dumps({'images': {'task': row}}))
                return SimpleNamespace(returncode=0)
            catalog, invoked = self.run_pool(root, 16, run, ['--retry-failed'])
            self.assertEqual(invoked.call_count, 1)
            self.assertEqual(catalog['images']['task']['status'], 'ready')
            self.assertEqual(json.loads(next((root / 'attempts').glob('*.json')).read_text()), old)
            self.assertEqual(json.loads((root / 'budget.json').read_text())['initial_used_bytes'], 23)


class SharedTaskScratchTests(unittest.TestCase):
    def test_layered_exports_are_bound_to_manifest_config_layers_and_source_identity(self):
        from prepare_shared_task_image import source_identity, validate_filesystem_export
        reference = 'registry/image@sha256:' + 'a' * 64
        layers = [{'digest': 'sha256:' + c * 64, 'size': 10} for c in 'bc']
        manifest = {'config': {'digest': 'sha256:' + 'd' * 64}, 'layers': layers}
        config = {'rootfs': {'diff_ids': ['sha256:' + c * 64 for c in 'ef']}}
        row = {'reference': reference, 'source_config': manifest['config']['digest'],
               'source_layers': layers, 'diff_ids': config['rootfs']['diff_ids']}
        validate_filesystem_export(row, reference, manifest, config)
        for key, value in [('reference', 'different'), ('source_layers', layers[:1]),
                           ('source_config', 'sha256:' + '0' * 64), ('diff_ids', list(reversed(row['diff_ids'])))]:
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'authenticated OCI input'):
                validate_filesystem_export({**row, key: value}, reference, manifest, config)
        exports = {'anchor': {'export_sha256': 'a' * 64}, 'target': {'export_sha256': 'b' * 64}}
        first = source_identity('ubuntu:22.04', reference, exports=exports)
        exports['target']['export_sha256'] = 'c' * 64
        self.assertNotEqual(first, source_identity('ubuntu:22.04', reference, exports=exports))

    def test_recovery_pin_is_part_of_identity_and_cannot_change_repository(self):
        from prepare_shared_task_image import source_identity
        pin = 'docker.io/library/ubuntu@sha256:' + 'a' * 64
        identity = source_identity('ubuntu:22.04', 'private-anchor', pin)
        self.assertEqual(identity['pinned_source'], pin)
        self.assertNotEqual(identity, source_identity('ubuntu:22.04', 'private-anchor'))
        with self.assertRaisesRegex(ValueError, 'another repository'):
            source_identity('python:3.11', 'private-anchor', pin)
        with self.assertRaises(ValueError):
            source_identity('ubuntu:22.04', 'private-anchor', 'ubuntu:latest')

    def test_cleanup_only_removes_marked_known_inputs_for_this_job(self):
        from prepare_shared_task_image import remove_abandoned_inputs
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, key in [('inputs-owned', 'wanted'), ('inputs-other-job', 'other'), ('inputs-user-files', 'wanted')]:
                path = root / name
                path.mkdir()
                (path / '.owner.json').write_text(json.dumps({'source_key': key}))
                (path / 'target.tar.gz').write_bytes(b'temporary input')
            (root / 'inputs-user-files' / 'notes.txt').write_text('preserve')
            (root / 'inputs-link').symlink_to(root / 'inputs-other-job', target_is_directory=True)
            remove_abandoned_inputs(root, 'wanted')
            self.assertFalse((root / 'inputs-owned').exists())
            self.assertTrue((root / 'inputs-other-job' / 'target.tar.gz').exists())
            self.assertTrue((root / 'inputs-user-files' / 'notes.txt').exists())
            self.assertTrue((root / 'inputs-link').is_symlink())


class SharedTaskPlanTests(unittest.TestCase):
    def test_explicit_cross_project_fallback_preserves_provenance_and_excludes_assigned_work(self):
        from plan_shared_task_pool import plan_shared
        anchor = 'aweaiteam/scaleswe:owner_repo_pr1'
        own = 'aweaiteam/scaleswe:owner_repo_pr2'
        other = 'aweaiteam/scaleswe:other_repo_pr2'
        excluded = 'aweaiteam/scaleswe:third_repo_pr1'
        ready = {'status': 'ready', 'reference': 'private@sha256:' + 'a' * 64,
                 'source_reference': 'docker.io/source@sha256:' + 'b' * 64, 'components': [{'bytes': 10}]}
        inventory = {'images': [{'source': source} for source in [anchor, own, other, excluded, 'ubuntu:22.04']]}
        catalog = {'schema': 1, 'images': {anchor: ready}}
        result = plan_shared(inventory, [catalog], fallback_anchor_source=anchor, exclude_sources=[excluded])
        rows = {row['source']: row for row in result['images']}
        self.assertEqual(set(rows), {own, other})
        self.assertEqual(rows[own]['anchor_strategy'], 'same_project')
        self.assertEqual(rows[other]['anchor_strategy'], 'shared_fallback')
        self.assertEqual(rows[other]['anchor_source_reference'], ready['source_reference'])
        self.assertEqual(result['anchor_projects'], 1)
        self.assertEqual(result['source_projects'], 2)
        ready['method'] = 'verified-flat-delta-v1'
        with self.assertRaisesRegex(ValueError, 'eligible retained original'):
            plan_shared(inventory, [catalog], fallback_anchor_source=anchor)

    def test_only_unprepared_same_project_sources_use_eligible_original_anchors(self):
        from plan_shared_task_pool import plan_shared
        anchor = 'aweaiteam/scaleswe:owner_repo_pr1'
        pending = 'aweaiteam/scaleswe:owner_repo_pr2'
        unrelated = 'aweaiteam/scaleswe:other_repo_pr2'
        ready = {'status': 'ready', 'reference': 'private@sha256:' + 'a' * 64,
                 'source_reference': 'docker.io/source@sha256:' + 'b' * 64,
                 'components': [{'bytes': 10}]}
        inventory = {'images': [{'source': value} for value in [anchor, pending, unrelated]]}
        catalog = {'schema': 1, 'images': {anchor: ready}}
        result = plan_shared(inventory, [catalog])
        self.assertEqual([r['source'] for r in result['images']], [pending])
        self.assertEqual(result['images'][0]['anchor_source'], anchor)
        self.assertEqual(result['images'][0]['anchor_source_reference'], ready['source_reference'])
        ready['method'] = 'verified-flat-delta-v1'
        self.assertEqual(plan_shared(inventory, [catalog])['images'], [])


if __name__ == '__main__':
    unittest.main()


class DeltaCompressionIdentityTests(unittest.TestCase):
    def test_resume_preserves_old_compressed_identity(self):
        from prepare_shared_task_image import source_identity
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertEqual(pool.job_compression_level(root, 6), 6)
            legacy = source_identity('ubuntu:22.04', 'anchor')
            self.assertNotIn('compression_level', legacy)
            (root / 'identity.json').write_text(json.dumps(legacy))
            self.assertEqual(pool.job_compression_level(root, 6), 9)
            optimized = source_identity('ubuntu:22.04', 'anchor', compression_level=6)
            self.assertNotEqual(legacy, optimized)
            (root / 'identity.json').write_text(json.dumps(optimized))
            self.assertEqual(pool.job_compression_level(root, 9), 6)
            with self.assertRaises(ValueError):
                source_identity('ubuntu:22.04', 'anchor', compression_level=0)


class CompactAnchorTests(unittest.TestCase):
    def test_compact_planning_uses_nearest_qualified_project_revision(self):
        from plan_shared_task_pool import plan_shared
        prefix = 'aweaiteam/scaleswe:owner_repo_pr'
        original = {'status': 'ready', 'reference': 'private@sha256:' + 'a' * 64,
                    'source_reference': 'docker.io/aweaiteam/scaleswe@sha256:' + 'b' * 64,
                    'components': [{'bytes': 10}]}
        compact = {**original, 'method': 'verified-flat-delta-v1',
                   'qualification': {'equivalent': True, 'mode': 'full_source_scan'}}
        catalogs = [{'schema': 1, 'images': {prefix + '1': original, prefix + '100': compact}}]
        inventory = {'images': [{'source': prefix + '101'}]}
        ordinary = plan_shared(inventory, catalogs)['images'][0]
        nearest = plan_shared(inventory, catalogs, allow_compact_anchors=True)['images'][0]
        self.assertEqual(ordinary['anchor_source'], prefix + '1')
        self.assertEqual(nearest['anchor_source'], prefix + '100')

    def test_compact_anchor_is_explicit_same_project_and_requires_full_proof(self):
        from plan_shared_task_pool import plan_shared
        anchor = 'aweaiteam/scaleswe:owner_repo_pr1'
        pending = 'aweaiteam/scaleswe:owner_repo_pr2'
        other = 'aweaiteam/scaleswe:other_repo_pr1'
        row = {'status': 'ready', 'method': 'verified-flat-delta-v1',
               'reference': 'private@sha256:' + 'a' * 64,
               'source_reference': 'docker.io/aweaiteam/scaleswe@sha256:' + 'b' * 64,
               'components': [{'bytes': 10}],
               'qualification': {'equivalent': True, 'mode': 'full_source_scan'}}
        inventory = {'images': [{'source': s} for s in [anchor, pending, other]]}
        catalogs = [{'schema': 1, 'images': {anchor: row}}]
        self.assertEqual(plan_shared(inventory, catalogs)['images'], [])
        result = plan_shared(inventory, catalogs, allow_compact_anchors=True)['images']
        self.assertEqual([r['source'] for r in result], [pending])
        self.assertEqual(result[0]['anchor_filesystem_source'], row['source_reference'])
        row['qualification']['equivalent'] = False
        self.assertEqual(plan_shared(inventory, catalogs, allow_compact_anchors=True)['images'], [])

    def test_original_anchor_source_metadata_is_digest_bound_and_flat(self):
        import hashlib
        from prepare_shared_task_image import flat_anchor_source, source_identity
        config = json.dumps({'os': 'linux', 'architecture': 'amd64', 'rootfs': {'diff_ids': ['sha256:' + 'c' * 64]}})
        layer = {'digest': 'sha256:' + 'b' * 64, 'size': 10}
        manifest = json.dumps({'config': {'digest': 'sha256:' + hashlib.sha256(config.encode()).hexdigest(), 'size': len(config)}, 'layers': [layer]})
        pin = 'docker.io/example/task@sha256:' + hashlib.sha256(manifest.encode()).hexdigest()
        resolved = {'reference': pin, 'manifest_json': manifest, 'config_json': config}
        self.assertEqual(flat_anchor_source(resolved, pin, 10), layer)
        with self.assertRaises(ValueError):
            flat_anchor_source(resolved, pin, 9)
        with self.assertRaises(ValueError):
            flat_anchor_source(resolved, 'docker.io/example/task@sha256:' + 'a' * 64, 10)
        with self.assertRaises(ValueError):
            flat_anchor_source({**resolved, 'manifest_json': manifest + ' '}, pin, 10)
        identity = source_identity('example/next:latest', 'private-anchor', anchor_filesystem_source=pin)
        self.assertEqual(identity['anchor_filesystem_source'], pin)
        with self.assertRaises(ValueError):
            source_identity('example/next:latest', 'private-anchor', anchor_filesystem_source='example/task:latest')
