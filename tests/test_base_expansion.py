import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
from plan_base_expansion import source_bases, missing_prefixes, write_plans, shared_sources


class BaseExpansionTests(unittest.TestCase):
    def item(self, source, family='MultiSWE', tasks=1, **extra):
        return {'source': source, 'families': [family], 'uses': [
            {'family': family, 'level': 'upstream_task_image', 'task_rows': tasks}], **extra}

    def ready(self, **extra):
        return {'status': 'ready', 'reference': 'private@sha256:'+'a'*64,
                'source_reference': 'public@sha256:'+'b'*64,
                'components': [{'bytes': 10}], **extra}

    def plan(self, items, ready=None, **options):
        return source_bases({'images': items}, {'images': ready or {}}, options.pop('metadata', {}), **options)

    def test_existing_project_gets_no_more_task_variants(self):
        rows = [self.item('org/ready:1'), self.item('org/ready:2'),
                self.item('org/missing:1'), self.item('org/missing:2')]
        result = self.plan(rows, {'org/ready:1': self.ready()})
        self.assertEqual([r['source'] for r in result['images']], ['org/missing:1'])
        self.assertEqual(result['images'][0]['potential_task_rows'], 2)

    def test_large_multilayer_base_prevents_redundant_project_seeding(self):
        rows = [self.item('org/a:1'), self.item('org/a:2')]
        for extra in [{}, {'method': 'verified-oci-delta-v1',
                           'qualification': {'equivalent': True, 'mode': 'full_source_scan'}}]:
            ready = self.ready(components=[{'bytes': 1024**3}]*10, **extra)
            self.assertEqual(self.plan(rows, {'org/a:1': ready})['images'], [])

    def test_fanout_order_is_balanced_across_families(self):
        rows = [self.item('org/multi:1', tasks=1000), self.item('org/multi2:1', tasks=100),
                self.item('org/r2e:1', family='R2E-Gym')]
        result = self.plan(rows, per_family=1)
        self.assertEqual([r['source'] for r in result['images']], ['org/multi:1', 'org/r2e:1'])

    def test_wrong_pin_and_wrong_recipe_do_not_hide_missing_bases(self):
        rows = [self.item('org/multi:1', pinned_source='expected'),
                self.item('smith:1', family='SWE-smith', preparation='swesmith-v1')]
        result = self.plan(rows, {r['source']: self.ready() for r in rows})
        self.assertEqual(len(result['images']), 2)
        self.assertEqual(result['images'][1]['preparation'], 'swesmith-v1')

    def test_assigned_project_is_skipped_but_not_counted_as_prepared(self):
        rows = [self.item('org/a:1'), self.item('org/a:2')]
        result = self.plan(rows, assigned=['org/a:2'])
        self.assertEqual(result['images'], [])
        self.assertEqual(result['summary']['existing_groups'], {})
        self.assertEqual(result['summary']['externally_assigned_groups'], {'MultiSWE': 1})

    def test_small_measured_representative_selected_and_large_only_group_deferred(self):
        rows = [self.item('org/a:1'), self.item('org/a:2'), self.item('org/b:1')]
        receipts = {'org/a:2': {'pin': {'reference': 'pin', 'compressed_bytes': 100}},
                    'org/b:1': {'big': {'reference': 'big', 'compressed_bytes': 3*1024**3}}}
        result = self.plan(rows, metadata=receipts)
        self.assertEqual([r['source'] for r in result['images']], ['org/a:2'])
        self.assertEqual(result['images'][0]['pinned_source'], 'pin')
        self.assertEqual(len(result['summary']['deferred_groups']), 1)

    def test_smith_environments_in_same_repo_are_distinct(self):
        rows = [self.item('smith:a', family='SWE-smith', repository='same'),
                self.item('smith:b', family='SWE-smith', repository='same')]
        self.assertEqual(len(self.plan(rows)['images']), 2)

    def test_prefixes_exclude_ready_and_assigned_and_deduplicate(self):
        def entry(key, tasks):
            return {'item': {'key': key, 'family': 'terminal-prefix', 'tasks': tasks}}
        a, b, c, d = entry('a', 10), entry('b', 9), entry('c', 8), entry('d', 7)
        selected, counts = missing_prefixes({'foundations': [a, a, b, c, d]},
            {'rows': [{'key': 'a', 'validated': True}]}, assigned=['b'], per_family=1)
        self.assertEqual(selected['terminal-prefix'], [c])
        self.assertEqual(counts['terminal-prefix']['missing_prefixes'], 2)
        self.assertEqual(counts['terminal-prefix']['selected_recipe_rows'], 8)

    def test_plan_directory_is_immutable(self):
        with TemporaryDirectory() as raw:
            output = Path(raw) / 'plan'
            write_plans(output, {'schema': 1, 'images': []}, {}, {'status': 'not_submitted'})
            self.assertEqual(json.loads((output/'sources/plan.json').read_text())['images'], [])
            with self.assertRaises(FileExistsError):
                write_plans(output, {}, {}, {})

    def test_only_measured_faithful_flat_sources_use_dependency_sharing(self):
        rows = [self.item('flat'), self.item('layered'), self.item('unknown'),
                self.item('smith', preparation='swesmith-v1')]
        metadata = {s: {'pin': {'reference': 'pin', 'layer_count': n}}
                    for s, n in [('flat', 1), ('layered', 2), ('smith', 1)]}
        normal, shared = shared_sources({'schema': 1, 'images': rows},
            {'images': {'anchor': self.ready()}}, metadata, 'anchor')
        self.assertEqual([r['source'] for r in shared['images']], ['flat'])
        self.assertEqual([r['source'] for r in normal['images']], ['layered', 'unknown', 'smith'])
        self.assertEqual(shared['images'][0]['anchor_strategy'], 'shared_fallback')
        with self.assertRaisesRegex(ValueError, 'faithful original'):
            shared_sources({'images': rows}, {'images': {'anchor': self.ready(method='unqualified')}}, metadata, 'anchor')
