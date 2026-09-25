from dataclasses import replace
import unittest
from unittest.mock import patch

from tests.test_control_plane import build_heartbeat, _sandbox_route
from ucloud_sandboxes import control_plane as cp
from ucloud_sandboxes.models import ResourceQuantity


class PlacementScoringTests(unittest.TestCase):
    def test_distinct_image_work_and_state_accounting(self):
        image = 'registry/image@sha256:' + 'a' * 64
        heartbeat = build_heartbeat(
            node_id='n', job_id='j', node_url='http://n',
            total_resources=ResourceQuantity(32, 65536, 1000000),
            cached_images=(image,),
        )
        states = ['creating', 'unknown', 'running', 'planned', 'quota_ready',
                  'rootfs_ready', 'parked', 'waking', 'failed', 'deleted']
        routes = [_sandbox_route(
            sandbox_id=f's{i}', node_id='n', job_id='j', node_url='http://n',
            state=state, resources=ResourceQuantity(1, 2048, 4096),
            spec={'id': f's{i}', 'image': image},
        ) for i, state in enumerate(states * 3)]
        with patch.object(cp, 'canonical_image_digest_ref', wraps=cp.canonical_image_digest_ref) as normalize:
            result = cp._node_placement_state(heartbeat, routes)
        # Once for the shared reference and once for its cache lookup, rather
        # than per sandbox on both projection passes.
        self.assertEqual(normalize.call_count, 2)
        self.assertEqual(result.active_creates, 15)
        self.assertEqual(result.assigned_shape_pressure, 24 / 32)
        self.assertEqual(result.inflight_image_identities, frozenset())
        self.assertEqual(result.projected_image_identities, frozenset({image}))
        self.assertEqual(result.available_resources, cp._node_available_resources(heartbeat, routes))
        unknown = cp._node_placement_state(replace(heartbeat, cached_images_known=False), routes)
        self.assertEqual(unknown.inflight_image_identities, frozenset({image}))

    def test_cache_aliases_missing_images_and_migration_reservations(self):
        digest = 'a' * 64
        tagged = f'registry/image:latest@sha256:{digest}'
        canonical = f'registry/image@sha256:{digest}'
        heartbeat = build_heartbeat(
            node_id='n', job_id='j', node_url='http://n',
            cached_images=(canonical,),
        )
        routes = [cp.PlacementReservation(
            reservation_id=str(i), node_id='n', job_id='j', node_url='http://n',
            resources=ResourceQuantity(1, 2048, 4096), image=image,
        ) for i, image in enumerate([tagged, canonical, 'missing', ''])]
        result = cp._node_placement_state(heartbeat, routes)
        self.assertEqual(result.inflight_image_identities, frozenset({'missing'}))
        self.assertEqual(result.projected_image_identities, frozenset({canonical, 'missing'}))
        self.assertEqual(result.active_creates, 4)
        self.assertEqual(result.available_resources, cp._node_available_resources(heartbeat, routes))


class InflightCreatePlacementTests(unittest.TestCase):
    def setUp(self):
        # Ranking is under test; worker admission has its own coverage.
        fit = patch.object(cp, '_node_can_fit_available', return_value=True)
        fit.start()
        self.addCleanup(fit.stop)

    def selector(self, heartbeats, routes=(), inflight=True):
        handler = object.__new__(cp.ControlPlaneHandler)
        handler.telemetry = None
        handler.registry_layer_cache = None
        handler.create_target_concurrency_per_node = 4
        handler.inflight_create_placements = (
            cp.InflightCreatePlacements() if inflight else None
        )
        handler._placement_routes = lambda: list(routes)
        handler._ready_sandbox_heartbeats = lambda **_kwargs: list(heartbeats)
        handler._nodes_with_image = lambda *_args, **_kwargs: set()
        return handler

    @staticmethod
    def heartbeats():
        return [build_heartbeat(
            node_id=name, job_id='job-' + name, node_url='http://' + name,
            total_resources=ResourceQuantity(16, 16384, 1000000),
        ) for name in ('a', 'b')]

    def test_unsettled_selections_spread_a_concurrent_burst(self):
        handler = self.selector(self.heartbeats())
        requested = ResourceQuantity(1, 1024, 4096)
        chosen = [handler._select_node(requested, claim_sandbox_id=f's{i}').node_id
                  for i in range(4)]
        self.assertEqual(sorted(chosen), ['a', 'a', 'b', 'b'])
        # Without a ledger every selection sees the same committed snapshot.
        herd = self.selector(self.heartbeats(), inflight=False)
        self.assertEqual(
            {herd._select_node(requested).node_id for _ in range(4)}, {'a'},
        )

    def test_release_and_commit_are_not_counted_twice(self):
        heartbeats = self.heartbeats()
        requested = ResourceQuantity(1, 1024, 4096)
        handler = self.selector(heartbeats)
        first = handler._select_node(requested, claim_sandbox_id='s0')
        self.assertEqual(first.node_id, 'a')
        handler.inflight_create_placements.release(first.job_id, 's0')
        self.assertEqual(handler._select_node(requested).node_id, 'a')
        # Once the route has committed, the unreleased claim is not added again.
        handler._select_node(requested, claim_sandbox_id='s1')
        committed = _sandbox_route(
            sandbox_id='s1', node_id='a', job_id='job-a', node_url='http://a',
            state='creating', resources=requested, spec={'id': 's1'},
        )
        handler._placement_routes = lambda: [committed]
        state = cp._node_placement_state(heartbeats[0], [committed])
        adjusted = handler.inflight_create_placements.adjusted(
            heartbeats[0], state, {'s1'},
        )
        self.assertEqual(adjusted, state)
        self.assertEqual(handler._select_node(requested).node_id, 'b')
