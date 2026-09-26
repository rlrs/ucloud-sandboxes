from http import HTTPStatus
import gzip
import io
import tarfile
import time
import unittest
from unittest.mock import Mock

from ucloud_sandboxes import control_plane
from ucloud_sandboxes.image_import import (
    ImageImportSubmitter,
    import_build_context,
    import_image_id,
)
from ucloud_sandboxes.routing import RoutingStore

from tests import test_control_plane as gateway_fixtures

EXTERNAL = "aweaiteam/scaleswe:materialsvirtuallab_monty_pr152"
MANAGED = "registry.internal:5000/ucloud-managed/import-x-1234:latest@sha256:" + "a" * 64


class ImageImportHelperTests(unittest.TestCase):
    def test_import_identity_and_context_are_deterministic(self):
        self.assertEqual(import_image_id(EXTERNAL), import_image_id(" " + EXTERNAL + " "))
        self.assertRegex(import_image_id(EXTERNAL), r"^import-[0-9a-f]{40}$")
        archive = import_build_context(EXTERNAL)
        self.assertEqual(archive, import_build_context(EXTERNAL))
        with tarfile.open(fileobj=io.BytesIO(gzip.decompress(archive))) as tar:
            self.assertEqual(tar.getnames(), ["Dockerfile"])
            self.assertEqual(tar.extractfile("Dockerfile").read(), f"FROM {EXTERNAL}\n".encode())

    def test_submitter_resubmits_only_after_the_interval(self):
        calls, now = [], [0.0]
        submitter = ImageImportSubmitter(
            lambda import_id, image: calls.append(import_id),
            interval_seconds=30, clock=lambda: now[0], background=False,
        )
        self.assertTrue(submitter.ensure_submitted("import-a", EXTERNAL))
        self.assertFalse(submitter.ensure_submitted("import-a", EXTERNAL))
        now[0] = 31
        self.assertTrue(submitter.ensure_submitted("import-a", EXTERNAL))
        self.assertEqual(calls, ["import-a", "import-a"])

    def test_a_failing_submission_is_logged_not_raised(self):
        def fail(*_):
            raise RuntimeError("registry down")
        submitter = ImageImportSubmitter(fail, background=False)
        with self.assertLogs("ucloud_sandboxes.image_import", level="WARNING"):
            self.assertTrue(submitter.ensure_submitted("import-a", EXTERNAL))


def handler(*, resolved=None, failure=""):
    subject = object.__new__(control_plane.ControlPlaneHandler)
    subject.registry_url = "http://registry.internal:5000"
    subject.registry_worker_url = "http://registry.internal:5000"
    subject.image_import_submitter = Mock()
    subject._resolve_sandbox_image_reference = Mock(return_value=(
        (resolved, None) if resolved else ("x", {"error_code": "image_id_not_found"})
    ))
    subject._image_import_failure = Mock(return_value=failure)
    return subject


class ExternalImageImportTests(unittest.TestCase):
    def test_without_immutable_workers_images_pass_through(self):
        subject = handler()
        subject.image_import_submitter = None
        self.assertEqual(subject._external_image_import(EXTERNAL, wait=True), (EXTERNAL, None))

    def test_managed_images_are_not_imported(self):
        subject = handler()
        self.assertEqual(subject._external_image_import(MANAGED, wait=True), (MANAGED, None))
        subject.image_import_submitter.ensure_submitted.assert_not_called()

    def test_a_published_import_replaces_the_external_reference(self):
        subject = handler(resolved=MANAGED)
        self.assertEqual(subject._external_image_import(EXTERNAL, wait=True), (MANAGED, None))
        subject._resolve_sandbox_image_reference.assert_called_once_with(
            import_image_id(EXTERNAL), reference_kind="name",
        )
        subject.image_import_submitter.ensure_submitted.assert_not_called()

    def test_creates_wait_for_an_import_and_preparation_does_not(self):
        subject = handler()
        image, pending = subject._external_image_import(EXTERNAL, wait=True)
        self.assertEqual((image, pending["error_code"], pending["retryable"]),
                         (EXTERNAL, "image_import_pending", True))
        subject.image_import_submitter.ensure_submitted.assert_called_with(
            import_image_id(EXTERNAL), EXTERNAL,
        )
        self.assertEqual(subject._external_image_import(EXTERNAL, wait=False), (EXTERNAL, None))

    def test_a_failed_import_is_reported_and_resubmitted(self):
        subject = handler(failure="manifest unknown")
        _, error = subject._external_image_import(EXTERNAL, wait=True)
        self.assertEqual(error["error_code"], "image_import_failed")
        self.assertFalse(error["retryable"])
        self.assertIn("manifest unknown", error["error"])
        subject.image_import_submitter.ensure_submitted.assert_called_once()


class GatewayImportTests(unittest.TestCase):
    def test_external_image_create_queues_an_import_build_and_retries(self):
        with gateway_fixtures._temporary_root() as root:
            gateway = gateway_fixtures._gateway_server(
                root,
                routing_file=root / "routes.sqlite",
                registry_url="http://registry.internal:5000",
                registry_worker_url="http://registry.internal:5000",
                import_external_images=True,
            )
            with gateway_fixtures._running_server(gateway) as base:
                response = gateway_fixtures.ControlPlaneTests()._json_request(
                    f"{base}/v1/sandboxes", method="POST",
                    payload={"id": "imported", "image": EXTERNAL, "cpus": 1, "memory_mb": 512},
                    allow_error=True,
                )
                self.assertEqual(response["status"], HTTPStatus.SERVICE_UNAVAILABLE)
                self.assertEqual(response["body"]["error_code"], "image_import_pending")
                self.assertEqual(response["headers"]["Retry-After"], "5")
                # The loopback submission reaches the ordinary build path; with
                # no builder the build is queued for the autoscaler.
                store = RoutingStore(root / "routes.sqlite")
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline and not store.pending_image_build_count():
                    time.sleep(0.05)
                self.assertEqual(store.pending_image_build_count(), 1)
                self.assertIn(import_image_id(EXTERNAL), [
                    item.image_id for item in store.load().image_builds.values()
                ])


if __name__ == "__main__":
    unittest.main()
