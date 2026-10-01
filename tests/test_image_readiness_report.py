import sys
from pathlib import Path
import unittest
sys.path.insert(0, str(Path(__file__).parents[1]/'scripts'))
from report_image_readiness import source_accounting, foundation_accounting, project


class ImageReadinessReportTests(unittest.TestCase):
    def item(self, source, family, rows=1, **extra):
        return {'source': source, 'uses': [{'family': family, 'level': 'upstream_task_image', 'task_rows': rows}], **extra}

    def ready(self, **extra):
        return {'status': 'ready', 'components': [{'bytes': 10}], **extra}

    def test_project_candidates_are_disjoint_from_ready_and_unprepared(self):
        items = [self.item('org/a:one', 'MultiSWE'), self.item('org/a:two', 'MultiSWE'),
                 self.item('org/b:one', 'MultiSWE')]
        report = source_accounting({'images': items}, {'images': {'org/a:one': self.ready()}})['families']['MultiSWE']
        self.assertEqual(report['task_image_rows_prepared'], 1)
        self.assertEqual(report['additional_rows_with_bounded_same_project_base'], 1)
        self.assertEqual(report['rows_without_bounded_same_project_base'], 1)
        self.assertFalse(report['same_project_delta_cost_qualified'])

    def test_same_repo_name_does_not_merge_different_owners_and_large_bases_are_separate(self):
        items = [self.item('org1/a:one', 'MultiSWE'), self.item('org1/a:two', 'MultiSWE'),
                 self.item('org2/a:one', 'MultiSWE')]
        report = source_accounting({'images': items}, {'images': {'org1/a:one': self.ready(components=[{'bytes': 3*1024**3}])}})['families']['MultiSWE']
        self.assertEqual(report['additional_rows_with_bounded_same_project_base'], 0)
        self.assertEqual(report['additional_rows_with_any_same_project_example'], 1)
        self.assertEqual(project({'source': 'swebench/sweb.eval.x86_64.org_1776_repo-name-123:latest'}, 'SWE-bench Verified'), 'org_1776_repo-name')

    def test_generic_base_uses_family_specific_fanout_not_combined_count(self):
        item = {'source': 'python:3', 'task_rows': 101, 'uses': [
            {'family': 'A', 'level': 'base_only', 'task_rows': 100},
            {'family': 'B', 'level': 'base_only', 'task_rows': 1}]}
        result = source_accounting({'images': [item]}, {'images': {'python:3': self.ready()}})['families']
        self.assertEqual(result['A']['generic_base_rows_prepared'], 100)
        self.assertEqual(result['B']['generic_base_rows_prepared'], 1)
        self.assertEqual(result['A']['task_image_rows_prepared'], 0)

    def test_changed_pin_or_enriched_recipe_is_not_ready(self):
        items = [self.item('org/a:one', 'MultiSWE', pinned_source='expected'),
                 self.item('org/b:one', 'MultiSWE')]
        result = source_accounting({'images': items}, {'images': {
            'org/a:one': self.ready(source_reference='different'),
            'org/b:one': self.ready(preparation='enriched')}})
        self.assertEqual(result['families']['MultiSWE']['task_image_rows_prepared'], 0)
        self.assertEqual(result['source_pin_mismatches'], ['org/a:one'])

    def test_foundations_deduplicate_keys_and_do_not_count_unvalidated_or_unplanned(self):
        entry = {'item': {'key': 'a', 'family': 'openswe', 'tasks': 10}}
        result = foundation_accounting({'foundations': [entry, entry]},
            [{'key': 'a', 'validated': True}, {'key': 'unplanned', 'validated': True}, {'key': 'bad', 'validated': False}])
        self.assertEqual(result['openswe']['prefixes'], 1)
        self.assertEqual(result['OpenSWE']['task_rows_with_prepared_prefix'], 10)
        self.assertEqual(result['OpenSWE']['task_rows_without_prepared_prefix'], 36874)
