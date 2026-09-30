import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
from cache_source_receipts import hydrate, load, pack, select, validated


class SourceReceiptTests(unittest.TestCase):
    def receipt(self):
        config = json.dumps({'os': 'linux', 'architecture': 'amd64', 'rootfs': {'diff_ids': ['sha256:' + 'a' * 64]},
                             'config': {'OnBuild': ['RUN required-step']}})
        manifest = json.dumps({'config': {'digest': 'sha256:' + hashlib.sha256(config.encode()).hexdigest(), 'size': len(config)},
                               'layers': [{'digest': 'sha256:' + 'b' * 64, 'size': 42}]})
        return {'reference': 'docker.io/example/source@sha256:' + hashlib.sha256(manifest.encode()).hexdigest(),
                'manifest_json': manifest, 'config_json': config, 'compressed_bytes': 0, 'onbuild': []}

    def test_authenticates_all_metadata_and_recomputes_derived_fields(self):
        source = 'example/source:latest'
        raw = self.receipt()
        result = validated(source, raw)
        self.assertEqual(result['compressed_bytes'], 42)
        self.assertEqual(result['onbuild'], ['RUN required-step'])
        for field in ['config_json', 'manifest_json']:
            with self.assertRaises(ValueError):
                validated(source, {**raw, field: raw[field] + ' '})
        with self.assertRaises(ValueError):
            validated('example/another:latest', raw)

    def test_roundtrip_contains_only_inputs_and_hydrates_both_layouts(self):
        source = 'example/source:latest'
        key = hashlib.sha256(source.encode()).hexdigest()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            origin = root / 'origin'
            origin.mkdir()
            (origin / (key + '.json')).write_text(json.dumps({'source': source, 'resolved': self.receipt(), 'private': 'discard'}))
            bundle = root / 'metadata.json.gz'
            self.assertEqual(pack([origin], bundle)['receipts'], 1)
            self.assertEqual(set(load(bundle)), {source})
            for layout in ['normal', 'shared']:
                destination = root / layout
                destination.mkdir()
                (destination / 'plan.json').write_text(json.dumps({'schema': 1, 'images': [{'source': source}]}))
                self.assertEqual(hydrate(bundle, destination, layout)['hydrated'], 1)
                self.assertEqual(hydrate(bundle, destination, layout)['hydrated'], 0)
                receipt = destination / (key + '.json') if layout == 'normal' else destination / 'work' / key / 'resolved.json'
                self.assertNotIn('private', json.loads(receipt.read_text()))

    def test_ambiguous_mutable_tags_require_a_pin(self):
        candidates = {'pin1': {'value': 1}, 'pin2': {'value': 2}}
        with self.assertRaises(ValueError):
            select('image:latest', None, candidates)
        self.assertEqual(select('image:latest', 'pin2', candidates), {'value': 2})
        self.assertIsNone(select('image:latest', 'unknown', candidates))
