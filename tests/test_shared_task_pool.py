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
    def run_pool(self, root, growth, run, extra=()):
        config = root / 'config.json'
        config.write_text('{}')
        args = ['pool', '--root', str(root), '--config', str(config), '--gateway', 'https://example.invalid',
                '--sdk-wheel', str(root / 'sdk.whl'), '--workers', '2', '--growth-limit-gib', str(growth), *extra]
        disk = SimpleNamespace(used_bytes=100, available_bytes=600 * 1024**3)
        with patch('sys.argv', args), patch('ucloud_sandboxes.config.DeploymentConfig.from_dict',
                   return_value=SimpleNamespace(control_state_file=lambda: root / 'state.sqlite')), \
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
