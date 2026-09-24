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
