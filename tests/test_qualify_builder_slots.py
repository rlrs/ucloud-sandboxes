import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import time
import unittest
from unittest.mock import AsyncMock, patch

from scripts import qualify_builder_slots as q


def fixture(index=0):
    return dict(index=index, recipe='python-agent', variant='app-change-5', context_sha256='a' * 64,
                context_path='/unused', build_args={})


class FakeClient:
    def __init__(self, *, submit_delay=0, wait_error=None):
        self.submit_delay = submit_delay
        self.wait_error = wait_error
        self.budgets = []
        self.get_calls = 0

    async def submit_image_build(self, image, *, timeout_seconds):
        self.budgets.append(timeout_seconds)
        await asyncio.sleep(self.submit_delay)
        return {'build_id': 'owned-build'}

    async def wait_for_image_build(self, identity, *, timeout_seconds, **kwargs):
        self.budgets.append(timeout_seconds)
        if self.wait_error:
            raise self.wait_error
        return {'build_id': identity, 'status': 'succeeded'}

    async def get_image_build(self, identity, **kwargs):
        self.get_calls += 1
        return {'build_id': identity, 'status': 'succeeded'}


class QualificationTests(unittest.TestCase):
    def test_candidate_requires_all_four_upgrade_receipts(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            nodes = ['101', '102', '103', '104']
            paths = []
            for node in nodes:
                path = root / (node + '.json')
                path.write_text(json.dumps(dict(complete=True, service_changed=True,
                    after=dict(job_id=node, node_epoch='epoch-' + node, finishing_capacity=2))))
                paths.append(path)
            self.assertEqual(q.verify_builder_receipts([], nodes, 4), [])
            self.assertEqual(len(q.verify_builder_receipts(paths, nodes, 6)), 4)
            with self.assertRaises(ValueError):
                q.verify_builder_receipts(paths[:3], nodes, 6)
            with self.assertRaises(ValueError):
                q.verify_builder_receipts(paths, ['201', *nodes[1:]], 6)
            paths[0].write_text(json.dumps(dict(complete=False)))
            with self.assertRaises(ValueError):
                q.verify_builder_receipts(paths, nodes, 6)

    def test_hold_cli_does_not_require_candidate_fields(self):
        with TemporaryDirectory() as temporary, patch.object(q, 'load_sdk', return_value=object()), \
                patch.object(q, 'hold_builders', AsyncMock(return_value={'completed': True})) as hold, \
                patch('builtins.print'):
            result = q.main(['hold', '--sdk-wheel', '/unused', '--output', temporary + '/new',
                             '--reservation', 'builder-slot-qualification-012345abcdef', '--duration', '1'])
            self.assertEqual(result, 0)
            hold.assert_awaited_once()

    def test_trace_distinguishes_packaging_wait_and_keeps_request_contents_private(self):
        import aiohttp
        async def scenario():
            trace = q.http_trace(aiohttp)
            record = {'http': [], '_arrival': time.monotonic() - .2}
            token = q.HTTP_RECORD.set(record)
            try:
                context = SimpleNamespace()
                params = SimpleNamespace(method='GET', url=SimpleNamespace(path='/v1/image-contexts/sha256:owned'))
                await trace.on_request_start[0](None, context, params)
                await trace.on_request_end[0](None, context, SimpleNamespace(response=SimpleNamespace(status=200)))
                first = record['context_preparation_seconds']
                self.assertGreaterEqual(first, .2)
                await trace.on_request_start[0](None, SimpleNamespace(), params)
                self.assertEqual(record['context_preparation_seconds'], first)
                self.assertEqual(record['http'][0]['status'], 200)
                self.assertNotIn('url', record['http'][0])
                self.assertNotIn('headers', record['http'][0])
            finally:
                q.HTTP_RECORD.reset(token)
        asyncio.run(scenario())

    def test_common_arrival_includes_local_wait_and_shares_remaining_budget(self):
        async def scenario():
            arrival = time.monotonic() - .15
            client = FakeClient(submit_delay=.02)
            row = await q.run_case(fixture(), client, lambda **kw: kw, prefix='slotq-test', arrival=arrival,
                                   deadline=arrival + 1, drain_deadline=arrival + 2)
            self.assertGreaterEqual(row['client_wall_seconds'], .17)
            self.assertLess(client.budgets[0], .86)
            self.assertLess(client.budgets[1], client.budgets[0] - .015)
            self.assertFalse(row['deadline_missed'])
        asyncio.run(scenario())

    def test_expired_before_local_admission_never_submits(self):
        async def scenario():
            client = FakeClient()
            start = time.monotonic() - 1
            row = await q.run_case(fixture(), client, lambda **kw: kw, prefix='slotq-test', arrival=start,
                                   deadline=start + .1, drain_deadline=time.monotonic() + .1)
            self.assertEqual(client.budgets, [])
            self.assertTrue(row['deadline_missed'])
            self.assertEqual(row['drain']['state'], 'never_posted')
        asyncio.run(scenario())

    def test_accepted_timeout_drains_but_remains_a_failed_deadline(self):
        async def scenario():
            start = time.monotonic()
            client = FakeClient(wait_error=TimeoutError('private detail must not persist'))
            row = await q.run_case(fixture(), client, lambda **kw: kw, prefix='slotq-test', arrival=start,
                                   deadline=start + 1, drain_deadline=start + 2)
            self.assertTrue(row['deadline_missed'])
            self.assertEqual(row['build']['status'], 'succeeded')
            self.assertEqual(row['drain']['state'], 'terminal_after_client_failure')
            self.assertEqual(client.get_calls, 1)
            self.assertNotIn('private detail', str(row))
            summary = q.summarize([dict(row, index=i) for i in range(48)], 1)
            self.assertEqual(summary['succeeded'], 48)
            self.assertEqual(summary['deadline_misses'], 48)
            self.assertFalse(summary['passed'])
        asyncio.run(scenario())

    def test_ambiguous_submission_is_reconciled_by_owned_image_name(self):
        class Ambiguous(FakeClient):
            async def submit_image_build(self, image, **kwargs):
                q.HTTP_RECORD.get()['http'].append({'category': 'submit', 'status': None})
                raise OSError('opaque transport')
        async def scenario():
            start = time.monotonic()
            client = Ambiguous()
            row = await q.run_case(fixture(), client, lambda **kw: kw, prefix='slotq-test', arrival=start,
                                   deadline=start + 1, drain_deadline=start + 2)
            self.assertEqual(row['build_id'], 'slotq-test-000')
            self.assertEqual(client.get_calls, 1)
            self.assertIn('error', row)
        asyncio.run(scenario())

    def test_drain_retries_404_and_reports_unresolved_when_budget_ends(self):
        class MissingError(Exception):
            status_code = 404
        class Missing(FakeClient):
            async def get_image_build(self, *args, **kwargs):
                self.get_calls += 1
                raise MissingError()
        async def scenario():
            client = Missing()
            value = await q.drain_case(client, 'owned', time.monotonic() + .025,
                                       poll_seconds=.005, observe=lambda _: None)
            self.assertGreater(client.get_calls, 1)
            self.assertEqual(value['state'], 'unresolved')
            self.assertGreater(value['errors']['404'], 1)
        asyncio.run(scenario())

    def test_cold_marker_precedes_dependencies_and_rejects_double_injection(self):
        source = 'FROM python@sha256:abc\nWORKDIR /work\nRUN python -m pip install x\n'
        changed = q.cold_dockerfile(source)
        self.assertLess(changed.index('UCLOUD_QUAL_NONCE'), changed.index('RUN python'))
        self.assertIn('/ucloud-qualification-cache-key', changed)
        with self.assertRaises(ValueError):
            q.cold_dockerfile(changed)

    def test_fixture_inventory_detects_changed_sources_and_rejects_symlinks(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'Dockerfile').write_text('FROM scratch\n')
            before = q.context_inventory(root)
            (root / 'fixture.json').write_text('ignored')
            self.assertEqual(before, q.context_inventory(root))
            (root / 'Dockerfile').write_text('FROM other\n')
            self.assertNotEqual(before['context_sha256'], q.context_inventory(root)['context_sha256'])
            (root / 'link').symlink_to('Dockerfile')
            with self.assertRaises(ValueError):
                q.context_inventory(root)

    def test_cold_arms_have_equal_source_bytes_and_48_distinct_changed_dependency_keys(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            frozen = []
            for recipe in q.RECIPES:
                for variant in q.VARIANTS:
                    context = root / 'contexts' / recipe / variant
                    context.mkdir(parents=True)
                    (context / 'Dockerfile').write_text('FROM pinned@sha256:abc\nRUN python -m pip install example\n')
                    manifest = dict(recipe=recipe, variant=variant, bases_digest_pinned=True,
                                    **q.context_inventory(context))
                    (context / 'fixture.json').write_text(json.dumps(manifest))
                    frozen.append(manifest)
            inventory = root / 'inventory.json'
            inventory.write_text(json.dumps(frozen))
            arms = []
            for arm in ('cold-a', 'cold-b'):
                output = root / arm
                result = q.prepare_fixtures(SimpleNamespace(source_root=root, inventory=inventory,
                    inventory_sha256=q.sha(inventory), output=output, mode='cold', arm=arm))
                self.assertEqual(result['cases'], 48)
                cases = json.loads((output / 'cases.json').read_text())['cases']
                keys = {v['build_args']['UCLOUD_QUAL_NONCE'] for v in cases}
                self.assertEqual(len(keys), 48)
                self.assertTrue(all(len(key) == 32 for key in keys))
                arms.append((cases, keys))
            self.assertFalse(arms[0][1] & arms[1][1])
            self.assertEqual([v['context_sha256'] for v in arms[0][0]],
                             [v['context_sha256'] for v in arms[1][0]])

    def test_progress_keeps_only_timing_and_classification(self):
        evidence = {}
        q.progress_evidence({'log_tail': '#12 [compile 3/8] RUN npm ci --private-url SECRET\n#12 1.23 secret stdout\n#12 DONE 2.4s\n#13 [4/8] COPY src ./src\n#13 CACHED'}, evidence)
        self.assertEqual(evidence['12'], {'operation': 'dependency', 'command_output_seen': True, 'done_seconds': 2.4})
        self.assertEqual(evidence['13'], {'cached': True})
        self.assertNotIn('SECRET', str(evidence))
        self.assertNotIn('secret stdout', str(evidence))


if __name__ == '__main__':
    unittest.main()
