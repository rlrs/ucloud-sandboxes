import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
sys.path.insert(0, str(Path(__file__).parents[1]/'scripts'))
from run_base_expansion import initialize_budget, stage_catalog, translated_growth_cap, GIB


class BaseExpansionRunnerTests(unittest.TestCase):
    def test_old_campaign_budget_never_exceeds_cumulative_allowance(self):
        for baseline, old in [(300*GIB+123, 100*GIB), (100*GIB, 101*GIB+1)]:
            cap=translated_growth_cap(baseline, old, 192)
            self.assertLessEqual(old+cap*GIB, baseline+192*GIB)
            self.assertLess(baseline+192*GIB-(old+cap*GIB), GIB)
        with self.assertRaisesRegex(ValueError,'incompatible'):
            translated_growth_cap(0, 300*GIB, 192)

    def test_restart_preserves_original_budget_and_rejects_changed_inputs(self):
        with TemporaryDirectory() as raw:
            path=Path(raw)/'state.json'
            first=initialize_budget(path,100,{'input':'hash'})
            self.assertEqual(initialize_budget(path,999,{'input':'hash'}),first)
            with self.assertRaisesRegex(ValueError,'inputs changed'):
                initialize_budget(path,999,{'input':'changed'})

    def test_all_stage_formats_share_baseline_and_reject_reset(self):
        with TemporaryDirectory() as raw:
            for kind in ['shared','normal','prefix']:
                root=Path(raw)/kind
                root.mkdir()
                stage_catalog(root,123,kind)
                path=root/('budget.json' if kind=='shared' else 'catalog.json')
                self.assertEqual(json.loads(path.read_text())['initial_used_bytes'],123)
                stage_catalog(root,123,kind)
                with self.assertRaisesRegex(ValueError,'baseline differs'):
                    stage_catalog(root,456,kind)
