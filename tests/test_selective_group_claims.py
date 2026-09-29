"""Early same-node claims avoid duplicate authenticated OCI preparation."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event
import unittest
from unittest.mock import patch

from tests import test_selective_environment_publication as fixture
from tests.test_environment_layers import FORMAT
from tests.test_oci_layer_materialize import directory, layer, member
from ucloud_sandboxes import environment_builder
from ucloud_sandboxes.build_deadline import ImageBuildTimeoutError, build_execution_deadline
from ucloud_sandboxes.environment_artifact import LAYER_TAG_PREFIX, layer_chain_id, layer_group_key
from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder, publication_metrics
from ucloud_sandboxes.environment_prepare import PreparationResult
from ucloud_sandboxes.oci_layer_materialize import materialize_layers


class SelectiveGroupClaimTests(unittest.TestCase):
    # Reuse the signed-registry/real-OCI fixture without inheriting its tests.
    setUp = fixture.SelectiveEnvironmentPublicationTests.setUp
    add_image = fixture.SelectiveEnvironmentPublicationTests.add_image
    mkfs = fixture.SelectiveEnvironmentPublicationTests.mkfs
    publish = fixture.SelectiveEnvironmentPublicationTests.publish
    tail = fixture.SelectiveEnvironmentPublicationTests.tail

    def other_builder(self):
        other = FreshEnvironmentBuilder(self.store, self.registry, self.keys.key, self.builder.work_root)
        other._layer_format = FORMAT
        other._mkfs = self.mkfs
        return other

    def reuse(self, builder, tag):
        with build_execution_deadline(3), publication_metrics() as metrics:
            result = builder._reuse_image_layers('ucloud-managed/' + tag, tag, max_groups=24)
            return result, dict(metrics)

    def assert_shared_preparation(self, isolated):
        tail = self.tail()
        self.add_image('first', [self.base, tail])
        self.add_image('second', [self.base, tail])
        other = self.other_builder()
        self.builder.preparation_subprocess = other.preparation_subprocess = isolated
        entered, release, second_waiting = Event(), Event(), Event()
        calls = []
        original_lock = other._group_lock

        @contextmanager
        def waiting(tag):
            second_waiting.set()
            with original_lock(tag):
                yield

        def prepare(client, repository, descriptors, diff_ids, root, **kwargs):
            calls.append(repository)
            entered.set()
            if not release.wait(2):
                raise TimeoutError('fixture preparation was not released')
            return materialize_layers(client, repository, descriptors, diff_ids, root, **kwargs)

        def child(client, repository, descriptors, diff_ids, counts, root, *, timeout_seconds):
            self.assertEqual(counts, [1])
            self.assertGreater(timeout_seconds, 0)
            return PreparationResult(tuple(prepare(client, repository, descriptors, diff_ids, root)),
                                     {'selective_subprocess_ms': 1.0})

        target = ('ucloud_sandboxes.environment_prepare.prepare_in_subprocess' if isolated
                  else 'ucloud_sandboxes.oci_layer_materialize.materialize_layers')
        with patch(target, side_effect=child if isolated else prepare), \
             patch.object(other, '_group_lock', side_effect=waiting), ThreadPoolExecutor(2) as pool:
            first = pool.submit(self.reuse, self.builder, 'first')
            self.assertTrue(entered.wait(2))
            second = pool.submit(self.reuse, other, 'second')
            try:
                self.assertTrue(second_waiting.wait(2))
            finally:
                release.set()
            first_result, first_metrics = first.result(timeout=4)
            second_result, second_metrics = second.result(timeout=4)
        self.assertEqual(first_result['components'], second_result['components'])
        self.assertEqual(calls, ['ucloud-managed/first'])
        self.assertEqual(self.client.opened, [('ucloud-managed/first', tail[0]['digest'])])
        self.assertEqual(len(self.mkfs_views), 1)
        self.assertEqual(first_metrics['groups_built'], 1)
        self.assertEqual(second_metrics['groups_reused'], 2)
        self.assertEqual(second_metrics['docker_pull_skipped'], 1)
        self.assertNotIn('selective_materializations', second_metrics)
        self.assertNotIn('selective_subprocess_ms', second_metrics)

    def test_threads_share_download_and_extraction_before_publication(self):
        self.assert_shared_preparation(False)

    def test_isolated_preparation_is_not_started_for_waiting_cache_hit(self):
        self.assert_shared_preparation(True)

    def large_tails(self):
        return [layer([directory('app'), member('app/' + name, name.encode() * 8192)], compressed=False)
                for name in ('first', 'second')]

    def test_multiple_missing_claims_are_sorted_and_held_before_extraction(self):
        tails = self.large_tails()
        self.add_image('ordered', [self.base, *tails])
        tags = [LAYER_TAG_PREFIX + layer_group_key(FORMAT,
                    layer_chain_id([self.base[1], *[entry[1] for entry in tails[:index]]]), [tail[1]])
                for index, tail in enumerate(tails)]
        acquired, held = [], set()
        original_lock = self.builder._group_lock

        @contextmanager
        def tracked(tag):
            self.assertNotIn(tag, held, 'must not reacquire a claim during publication')
            with original_lock(tag):
                acquired.append(tag)
                held.add(tag)
                try:
                    yield
                finally:
                    held.remove(tag)

        def prepare(*args, **kwargs):
            self.assertEqual(held, set(tags))
            return materialize_layers(*args, **kwargs)

        with patch.object(self.builder, '_group_lock', side_effect=tracked), \
             patch('ucloud_sandboxes.oci_layer_materialize.materialize_layers', side_effect=prepare):
            result, metrics = self.reuse(self.builder, 'ordered')
        self.assertEqual(acquired, sorted(tags))
        self.assertFalse(held)
        self.assertEqual(metrics['groups_built'], 2)
        self.assertEqual(len(result['components']), 3)

    def test_deadline_wait_releases_already_acquired_claim(self):
        groups = [('base', ['base'], None, 0, 1),
                  ('layer-a', ['a'], 'base', 1, 2), ('layer-z', ['z'], 'a', 2, 3)]

        def attempt():
            with build_execution_deadline(.1):
                self.builder._materialize_registry_groups('fixture', [], groups,
                    ['cached', None, None], 'image', {}, [], FORMAT)

        with self.builder._group_lock('layer-z'), \
             patch.object(self.builder, '_reuse_layer_component', return_value=None), \
             patch.object(self.builder, '_materialize_claimed_registry_groups') as prepare, \
             ThreadPoolExecutor(1) as pool:
            future = pool.submit(attempt)
            with self.assertRaises(ImageBuildTimeoutError):
                future.result(timeout=2)
            prepare.assert_not_called()
            with build_execution_deadline(.1), self.builder._group_lock('layer-a'):
                pass

    def test_claim_filled_while_waiting_is_released_before_other_preparation(self):
        groups = [('base', ['base'], None, 0, 1),
                  ('layer-a', ['filled'], 'base', 1, 2), ('layer-z', ['missing'], 'filled', 2, 3)]
        held, acquired = set(), []

        @contextmanager
        def claim(tag):
            self.assertFalse(held, 'a newly cached claim must not block later preparation')
            held.add(tag)
            acquired.append(tag)
            try:
                yield
            finally:
                held.remove(tag)

        def prepare(_repository, _source, _groups, components, *_args):
            self.assertEqual(held, {'layer-z'})
            self.assertEqual(components, ['cached-base', 'cached-filled', None])
            return 'prepared'

        with patch.object(self.builder, '_group_lock', side_effect=claim), \
             patch.object(self.builder, '_reuse_layer_component', side_effect=['cached-filled', None]), \
             patch.object(self.builder, '_materialize_claimed_registry_groups', side_effect=prepare):
            result = self.builder._materialize_registry_groups('fixture', [], groups,
                ['cached-base', None, None], 'image', {}, [], FORMAT)
        self.assertEqual(result, 'prepared')
        self.assertEqual(acquired, ['layer-a', 'layer-z'])
        self.assertFalse(held)

    def test_fallback_releases_claim_before_docker_path_reacquires_it(self):
        self.add_image('whiteout', [self.base, layer([member('.wh.deleted')])])
        with build_execution_deadline(2), patch.object(self.store, '_checked', wraps=self.store._checked) as pull:
            _, environment = self.publish('whiteout')
        pull.assert_called_once()
        self.assertEqual(environment.components[0], self.base_component)
        self.assertEqual(len(self.mkfs_views), 1)
        self.assertIn('docker-materialized', self.mkfs_views[0])

    def test_failed_later_publication_preserves_completed_group_and_releases_claims(self):
        tails = self.large_tails()
        self.add_image('partial', [self.base, *tails])
        count = 0

        def fail_second(*args, **kwargs):
            nonlocal count
            count += 1
            if count == 2:
                raise OSError('fixture second mkfs failed')
            return self.mkfs(*args, **kwargs)

        with patch.object(self.builder, '_mkfs', side_effect=fail_second), self.assertRaises(OSError):
            self.reuse(self.builder, 'partial')
        self.assertEqual([digest for _, digest in self.client.opened], [tail[0]['digest'] for tail in tails])
        self.client.opened.clear()
        result, metrics = self.reuse(self.builder, 'partial')
        self.assertEqual(self.client.opened, [('ucloud-managed/partial', tails[1][0]['digest'])])
        self.assertEqual(metrics['groups_reused'], 2)
        self.assertEqual(metrics['groups_built'], 1)
        self.assertEqual(len(result['components']), 3)
        self.assertEqual([path.name for path in self.builder.work_root.iterdir()], ['layer-locks'])

    def test_cached_group_is_refreshed_after_claim_wait_and_gc_failure_stops_signing(self):
        self.add_image('swept', [self.base, self.tail()])
        original = self.builder._reuse_layer_component

        def swept(tag, diff_ids, parent, layer_format, *, refresh=True):
            if tuple(diff_ids) == (self.base[1],) and refresh:
                return None
            return original(tag, diff_ids, parent, layer_format, refresh=refresh)

        with patch.object(self.builder, '_reuse_layer_component', side_effect=swept), \
             patch.object(environment_builder, 'sign_layer_component', side_effect=AssertionError('must not sign')):
            result, _metrics = self.reuse(self.builder, 'swept')
        self.assertIsNone(result)
        result, metrics = self.reuse(self.builder, 'swept')
        self.assertIsNotNone(result)
        self.assertEqual(metrics['groups_built'], 1)


if __name__ == '__main__':
    unittest.main()
