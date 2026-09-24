import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import orjson

from tests.test_control_plane import _sandbox_route
from ucloud_sandboxes.routing import RoutingStore


class FleetJsonDecoderTests(unittest.TestCase):
    def test_native_envelope_preserves_nested_json_and_large_generations(self):
        with TemporaryDirectory() as directory:
            store = RoutingStore(Path(directory) / 'routes.sqlite')
            route = _sandbox_route(
                sandbox_id='json-test', node_id='n', job_id='j', node_url='http://n',
                generation=2**63 - 1,
                spec={'id': 'json-test', 'env': {'unicode': 'é🌍', 'escaped': '\ud800'},
                      'large_integer': 2**100, 'value': float('inf')},
            )
            store.upsert_sandbox(route)
            native = store._sandbox_route_rows_readonly()
            with patch('ucloud_sandboxes.routing.orjson.loads', json.loads):
                standard = store._sandbox_route_rows_readonly()
            self.assertEqual(native, standard)
            self.assertEqual(native[0]['generation'], 2**63 - 1)
            restored = store.sandbox_routes_readonly()[0]
            self.assertEqual(restored.spec, route.spec)
            self.assertEqual(restored.generation, route.generation)

    def test_empty_fleet(self):
        with TemporaryDirectory() as directory:
            store = RoutingStore(Path(directory) / 'routes.sqlite')
            self.assertEqual(store.sandbox_routes_readonly(), [])

    def test_native_rejection_preserves_standard_decoder_fallback(self):
        with TemporaryDirectory() as directory:
            store = RoutingStore(Path(directory) / 'routes.sqlite')
            with patch('ucloud_sandboxes.routing.orjson.loads',
                       side_effect=orjson.JSONDecodeError('rejected', '', 0)):
                self.assertEqual(store.sandbox_routes_readonly(), [])
