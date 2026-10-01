import gzip
import io
import json
from pathlib import Path
import sys
import tarfile
from tempfile import TemporaryDirectory
import unittest

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
from prepare_shared_task_image import read_flat_index, write_flat_index
from score_shared_task_anchors import rank_indices
from tests.test_oci_flat_delta import ROOT, archive
from ucloud_sandboxes.oci_flat_delta import index_flat_tar


class SharedAnchorScoreTests(unittest.TestCase):
    def test_scores_actual_retained_file_reuse_and_rejects_tampered_index(self):
        common = ('app/dependency', tarfile.REGTYPE, 'x' * 10000, 0o644)
        target = index_flat_tar(io.BytesIO(archive(ROOT + [common, ('app/task', tarfile.REGTYPE, 'new', 0o644)])))
        good = index_flat_tar(io.BytesIO(archive(ROOT + [common])))
        bad = index_flat_tar(io.BytesIO(archive(ROOT)))
        ranked = rank_indices(target, [({'anchor_source': 'unrelated'}, bad), ({'anchor_source': 'similar'}, good)])
        self.assertEqual(ranked[0]['anchor_source'], 'similar')
        self.assertEqual(ranked[0]['changed_regular_file_bytes'], 3)
        self.assertEqual(ranked[1]['changed_regular_file_bytes'], 10003)
        with TemporaryDirectory() as raw:
            path = Path(raw) / 'index.json.gz'
            layer = {'digest': 'sha256:' + 'a' * 64, 'size': 10}
            write_flat_index(path, layer, target)
            self.assertEqual(read_flat_index(path, layer), target)
            payload = json.loads(gzip.decompress(path.read_bytes()))
            payload['payload']['entries'][0]['mode'] ^= 1
            path.write_bytes(gzip.compress(json.dumps(payload).encode()))
            with self.assertRaisesRegex(ValueError, 'identity'):
                read_flat_index(path, layer)
