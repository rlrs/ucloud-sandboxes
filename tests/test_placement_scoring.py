from dataclasses import replace
import unittest
from unittest.mock import Mock, patch

from tests.gateway_support import gateway_services
from tests.test_control_plane import build_heartbeat, _sandbox_route
from ucloud_sandboxes import control_plane as cp
from ucloud_sandboxes.gateway import fleet, placement as placement_rules
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
        normalize = Mock(wraps=placement_rules.canonical_image_digest_ref)
        with (patch.object(placement_rules, 'canonical_image_digest_ref', normalize),
              patch.object(fleet, 'canonical_image_digest_ref', normalize)):
            result = placement_rules._node_placement_state(heartbeat, routes)
        # Once for the shared reference and once for its cache lookup, rather
        # than per sandbox on both projection passes.
        self.assertEqual(normalize.call_count, 2)
        self.assertEqual(result.active_creates, 15)
        self.assertEqual(result.assigned_shape_pressure, 24 / 32)
        self.assertEqual(result.inflight_image_identities, frozenset())
        self.assertEqual(result.projected_image_identities, frozenset({image}))
        self.assertEqual(result.available_resources, cp._node_available_resources(heartbeat, routes))
        unknown = placement_rules._node_placement_state(replace(heartbeat, cached_images_known=False), routes)
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
        result = placement_rules._node_placement_state(heartbeat, routes)
        self.assertEqual(result.inflight_image_identities, frozenset({'missing'}))
        self.assertEqual(result.projected_image_identities, frozenset({canonical, 'missing'}))
        self.assertEqual(result.active_creates, 4)
        self.assertEqual(result.available_resources, cp._node_available_resources(heartbeat, routes))


class InflightCreatePlacementTests(unittest.TestCase):
    def setUp(self):
        # Ranking is under test; worker admission has its own coverage.
        fit = patch.object(placement_rules, '_node_can_fit_available', return_value=True)
        fit.start()
        self.addCleanup(fit.stop)

    def selector(self, heartbeats, routes=(), inflight=True):
        placement = gateway_services(create_target_concurrency_per_node=4).placement
        if not inflight:
            placement.inflight = None
        placement.routes = lambda: list(routes)
        placement.fleet.ready_sandbox_heartbeats = lambda **_kwargs: list(heartbeats)
        placement.image_locality = lambda *_args, **_kwargs: set()
        return placement

    @staticmethod
    def heartbeats():
        return [build_heartbeat(
            node_id=name, job_id='job-' + name, node_url='http://' + name,
            total_resources=ResourceQuantity(16, 16384, 1000000),
        ) for name in ('a', 'b')]

    def test_unsettled_selections_spread_a_concurrent_burst(self):
        placement = self.selector(self.heartbeats())
        requested = ResourceQuantity(1, 1024, 4096)
        chosen = [placement.select(requested, claim_sandbox_id=f's{i}').node_id
                  for i in range(4)]
        self.assertEqual(sorted(chosen), ['a', 'a', 'b', 'b'])
        # Without a ledger every selection sees the same committed snapshot.
        herd = self.selector(self.heartbeats(), inflight=False)
        self.assertEqual(
            {herd.select(requested).node_id for _ in range(4)}, {'a'},
        )

    def test_release_and_commit_are_not_counted_twice(self):
        heartbeats = self.heartbeats()
        requested = ResourceQuantity(1, 1024, 4096)
        placement = self.selector(heartbeats)
        first = placement.select(requested, claim_sandbox_id='s0')
        self.assertEqual(first.node_id, 'a')
        placement.inflight.release(first.job_id, 's0')
        self.assertEqual(placement.select(requested).node_id, 'a')
        # Once the route has committed, the unreleased claim is not added again.
        placement.select(requested, claim_sandbox_id='s1')
        committed = _sandbox_route(
            sandbox_id='s1', node_id='a', job_id='job-a', node_url='http://a',
            state='creating', resources=requested, spec={'id': 's1'},
        )
        placement.routes = lambda: [committed]
        state = placement_rules._node_placement_state(heartbeats[0], [committed])
        adjusted = placement.inflight.adjusted(
            heartbeats[0], state, {'s1'},
        )
        self.assertEqual(adjusted, state)
        self.assertEqual(placement.select(requested).node_id, 'b')
