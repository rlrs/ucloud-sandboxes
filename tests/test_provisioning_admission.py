import unittest

from tests import test_control_plane as helpers
from ucloud_sandboxes.routing import RoutingStore

TEST_TIER = "contract"


class ProvisioningAdmissionTests(unittest.TestCase):
    _request = helpers.ControlPlaneTests._json_request

    def test_cold_builder_submission_keeps_demand_and_marks_safe_retry(self):
        with helpers._temporary_root() as root:
            gateway = helpers._gateway_server(root, routing_file=root / "routes.sqlite")
            context = helpers._store_build_context(gateway, helpers._tar_gz_context({"Dockerfile": b"FROM scratch\n"}))
            with helpers._running_server(gateway) as base:
                result = self._request(f"{base}/v1/images/build", method="POST", allow_error=True,
                    payload={"id": "cold-build", "tag": "example/cold-build", "wait": False, **context})
            self.assertEqual(result["status"], 503)
            self.assertEqual(result["body"]["error_code"], "builder_not_ready")
            self.assertTrue(result["body"]["retryable"])
            self.assertEqual(result["headers"]["Retry-After"], "2")
            self.assertEqual(RoutingStore(root / "routes.sqlite").pending_image_build_count(), 1)
