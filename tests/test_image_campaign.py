import gzip
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from ucloud_sandboxes.image_foundations import terminal_foundation

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
with patch.object(sys, 'path', [str(SCRIPTS), *sys.path]):
    spec = importlib.util.spec_from_file_location('campaign', SCRIPTS / 'image_campaign.py')
    campaign = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(campaign)
    import prepare_image_pool as pool
    import prepare_image_foundations as foundations

BASE = 'docker.io/library/ubuntu@sha256:' + 'a' * 64


class ImageCampaignTests(unittest.TestCase):
    def inputs(self, root):
        sources = root / 'sources'
        sources.mkdir()
        item = {'source': 'ubuntu:22.04', 'families': ['example'], 'task_rows': 5}
        (sources / 'plan.json').write_text(json.dumps({'schema': 1, 'images': [item]}))
        (sources / 'catalog.json').write_text(json.dumps({'images': {'ubuntu:22.04': {
            'source_reference': BASE, 'reference': 'private-registry/old', 'credential': 'exclude-me'}}}))
        foundation = terminal_foundation('FROM ubuntu:22.04\nRUN apt-get update\nCOPY task_file /app\n',
                                        source_base='ubuntu:22.04', resolved_base={'reference': BASE, 'onbuild': []})
        foundations = root / 'foundations'
        context = foundations / foundation.image_id
        context.mkdir(parents=True)
        (context / 'Dockerfile').write_text(foundation.dockerfile)
        (foundations / 'plan.json').write_text(json.dumps({'schema': 1, 'revision': 'commit', 'foundations': [{
            'key': foundation.key, 'image_id': foundation.image_id, 'family': 'terminal-prefix', 'tasks': 5}]}))
        return sources, foundations, foundation

    def test_offline_roundtrip_excludes_stale_state_and_preserves_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources, foundations, foundation = self.inputs(root)
            archive = root / 'inputs.json.gz'
            report = campaign.pack(sources, [foundations], archive)
            self.assertEqual(report['pinned_sources'], 1)
            self.assertNotIn(b'exclude-me', gzip.decompress(archive.read_bytes()))
            restored = root / 'restored'
            campaign.materialize(archive, restored, 'recovery-1')
            plan = json.loads((restored / 'sources/plan.json').read_text())
            self.assertEqual(plan['images'][0]['pinned_source'], BASE)
            self.assertEqual(plan['rebuild_generation'], 'recovery-1')
            self.assertFalse(list(restored.rglob('catalog.json')))
            self.assertEqual((restored / 'terminal-prefix' / foundation.image_id / 'Dockerfile').read_bytes(),
                             (foundations / foundation.image_id / 'Dockerfile').read_bytes())
            commands = campaign.commands(restored, 'https://example.invalid', Path('/wheel with space'), Path('/config'), '/python')
            self.assertIn('--growth-limit-gib 1200', commands[-1])
            self.assertIn("'/wheel with space'", commands[-1])
            with self.assertRaises(ValueError):
                campaign.materialize(archive, restored, 'another')

    def test_corrupt_bundle_and_changed_context_fail_before_rebuild(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources, foundations, foundation = self.inputs(root)
            archive = root / 'inputs.json.gz'
            campaign.pack(sources, [foundations], archive)
            data = json.loads(gzip.decompress(archive.read_bytes()))
            data['payload']['sources'][0]['source'] = 'different:latest'
            archive.write_bytes(gzip.compress(json.dumps(data).encode()))
            with self.assertRaisesRegex(ValueError, 'checksum'):
                campaign.materialize(archive, root / 'restored', 'recovery-1')
            self.assertFalse((root / 'restored').exists())
            (foundations / foundation.image_id / 'Dockerfile').write_text('FROM different\n')
            with self.assertRaisesRegex(ValueError, 'changed'):
                campaign.pack(sources, [foundations], root / 'new.gz')

    def test_recovery_identity_cannot_reuse_lost_volume_publication(self):
        old = pool.image_identity(BASE)
        new = pool.image_identity(BASE, generation='recovery-1')
        self.assertNotEqual(old, new)
        self.assertEqual(new, pool.image_identity(BASE, generation='recovery-1'))
        self.assertIsNone(pool.catalog_publication({'status': 'ready', 'key': old,
            'image_id': 'precomputed-' + old[:32]}, new))
        for generation in ('../escape', 1, 'A'):
            with self.assertRaises(ValueError):
                pool.rebuild_generation({'rebuild_generation': generation})
        item = {'key': 'a' * 64, 'image_id': 'foundation-terminal-prefix-' + 'a' * 32,
                'family': 'terminal-prefix'}
        identity = foundations.build_image_id(item, 'r' * 32)
        self.assertLessEqual(len(identity), 64)
        self.assertNotEqual(identity, item['image_id'])
