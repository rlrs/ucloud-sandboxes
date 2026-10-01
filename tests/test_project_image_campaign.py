import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
from prepare_project_image_campaign import seed_plan, stage_inputs


class ProjectCampaignTests(unittest.TestCase):
    def test_seeds_once_per_missing_project_without_overlapping_known_work(self):
        source = 'aweaiteam/scaleswe:base_repo_pr1'
        original = {'status': 'ready', 'reference': 'private@sha256:' + 'a' * 64,
                    'source_reference': 'docker.io/aweaiteam/scaleswe@sha256:' + 'b' * 64,
                    'components': [{'bytes': 10}]}
        compact = {**original, 'method': 'verified-flat-delta-v1',
                   'qualification': {'equivalent': True, 'mode': 'full_source_scan'}}
        catalog = {'schema': 1, 'images': {source: original, 'aweaiteam/scaleswe:known_repo_pr1': compact}}
        rows = [source, 'aweaiteam/scaleswe:known_repo_pr2', 'aweaiteam/scaleswe:new_repo_pr1',
                'aweaiteam/scaleswe:new_repo_pr2', 'aweaiteam/scaleswe:other_repo_pr1']
        plan = seed_plan({'images': [{'source': s} for s in rows]}, [catalog], source, [rows[-1]])
        self.assertEqual([r['source'] for r in plan['images']], ['aweaiteam/scaleswe:new_repo_pr1'])

    def test_phases_share_original_budget_and_existing_assignment_survives_resume(self):
        with TemporaryDirectory() as raw:
            root = Path(raw)
            baseline = {'initial_used_bytes': 123}
            first = {'schema': 1, 'images': [{'source': 'first'}]}
            second = {'schema': 1, 'images': [{'source': 'first', 'anchor': 'different'}, {'source': 'second'}]}
            stage_inputs(root / 'seeds', first, baseline)
            resumed = stage_inputs(root / 'seeds', second, baseline)
            self.assertEqual(resumed['images'], [{'source': 'first'}, {'source': 'second'}])
            stage_inputs(root / 'tasks', second, baseline)
            self.assertEqual(json.loads((root / 'tasks/budget.json').read_text()), baseline)
            with self.assertRaisesRegex(ValueError, 'baseline'):
                stage_inputs(root / 'tasks', second, {'initial_used_bytes': 124})
