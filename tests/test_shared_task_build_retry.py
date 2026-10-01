import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
from prepare_shared_task_image import complete_build


class SharedBuildRetryTests(unittest.TestCase):
    def test_retry_archives_terminal_failure_and_journals_new_acceptance(self):
        with TemporaryDirectory() as raw:
            receipt = Path(raw) / 'build.json'
            receipt.write_text(json.dumps({'build_id': 'old', 'status': 'queued'}))
            client = Mock()
            client.submit_image_build.return_value = {'build_id': 'new', 'status': 'queued'}
            def wait(build_id, **kwargs):
                self.assertEqual(json.loads(receipt.read_text())['build_id'], build_id)
                return {'build_id': build_id, 'status': 'failed' if build_id == 'old' else 'succeeded'}
            client.wait_for_image_build.side_effect = wait
            result, reused = complete_build(client, object(), receipt, retry_failed=True)
            self.assertEqual(result['build_id'], 'new')
            self.assertFalse(reused)
            client.submit_image_build.assert_called_once()
            self.assertEqual(len(list((Path(raw) / 'attempts').glob('*.json'))), 1)

    def test_wait_timeout_never_submits_duplicate(self):
        with TemporaryDirectory() as raw:
            receipt = Path(raw) / 'build.json'
            receipt.write_text(json.dumps({'build_id': 'running', 'status': 'queued'}))
            client = Mock()
            client.wait_for_image_build.side_effect = TimeoutError('still running')
            with self.assertRaises(TimeoutError):
                complete_build(client, object(), receipt, retry_failed=True)
            client.submit_image_build.assert_not_called()

    def test_reuses_success_and_default_preserves_failure(self):
        for status in ('succeeded', 'failed'):
            with self.subTest(status=status), TemporaryDirectory() as raw:
                receipt = Path(raw) / 'build.json'
                build = {'build_id': 'old', 'status': status, 'image': {'manifest_digest': 'sha256:abc'}}
                receipt.write_text(json.dumps(build))
                client = Mock()
                client.wait_for_image_build.return_value = build
                if status == 'succeeded':
                    self.assertEqual(complete_build(client, object(), receipt), (build, True))
                    client.wait_for_image_build.assert_not_called()
                else:
                    with self.assertRaises(RuntimeError):
                        complete_build(client, object(), receipt)
                client.submit_image_build.assert_not_called()
