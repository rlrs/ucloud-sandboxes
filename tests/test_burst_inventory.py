"""Status polling must stay independent of cold-start and storage work."""

from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from tests import test_control_plane as gateway_fixtures
from tests import test_direct_provisioner as direct_fixtures
from ucloud_sandboxes.control_plane import ControlPlaneHandler
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.node_agent import NodeAgentHandler
from ucloud_sandboxes.sandbox import SandboxRestoreBusyError


class BurstInventoryTests(unittest.TestCase):
    def test_filtered_inventory_avoids_full_scan_and_storage(self):
        with TemporaryDirectory() as directory:
            fixture = direct_fixtures.DirectProvisionerTests()
            provisioner, *_ = fixture.make(Path(directory).resolve())
            service = DirectSandboxService(provisioner)
            fixture.create(service, fixture.spec())
            service.park("sandbox", operation_id="park:test")
            handler = SimpleNamespace(
                path="/v1/sandboxes?sandbox_id=sandbox",
                sandboxes_enabled=True,
                manager=SimpleNamespace(
                    service=service, list=Mock(side_effect=AssertionError)
                ),
                _check_node_control_authorized=lambda: True,
                _write_json=Mock(),
            )
            handler._sandbox_inventory_payload = lambda record: (
                NodeAgentHandler._sandbox_inventory_payload(handler, record)
            )
            with (
                patch.object(
                    service.warden, "_storage_record", side_effect=AssertionError
                ),
                patch.object(service.warden, "inspect", side_effect=AssertionError),
                patch.object(
                    provisioner.registry, "snapshot", side_effect=AssertionError
                ),
            ):
                NodeAgentHandler.do_GET.__wrapped__(handler)
            record = handler._write_json.call_args.args[0]["sandboxes"][0]
            self.assertEqual(record["spec"]["id"], "sandbox")
            self.assertEqual(record["state"], "parked")
            self.assertNotIn("storage_snapshot", record)

            snapshot = gateway_fixtures._portable_snapshot("sandbox", generation=7)
            service._remember_published_snapshot(snapshot)
            NodeAgentHandler.do_GET.__wrapped__(handler)
            self.assertEqual(
                handler._write_json.call_args.args[0]["sandboxes"][0][
                    "storage_snapshot"
                ],
                snapshot.to_dict(),
            )
            service._forget_published_snapshot("sandbox", 7)
            handler.path = "/v1/sandboxes?sandbox_id=missing"
            NodeAgentHandler.do_GET.__wrapped__(handler)
            self.assertEqual(handler._write_json.call_args.args[0], {"sandboxes": []})

    def test_gateway_filters_request_and_accepts_legacy_full_inventory(self):
        record = {"spec": {"id": "target"}, "state": "running"}
        response = SimpleNamespace(
            status=200, json=lambda: {"sandboxes": [{"spec": {"id": "other"}}, record]}
        )
        handler = SimpleNamespace(_proxy_request=Mock(return_value=response))
        self.assertEqual(
            ControlPlaneHandler._sandbox_record_on_node(
                handler, "http://node", "target"
            ),
            record,
        )
        self.assertEqual(
            handler._proxy_request.call_args.args[1], "/v1/sandboxes?sandbox_id=target"
        )

    def test_restore_rejection_has_explicit_retry_contract(self):
        handler = SimpleNamespace(_write_json=Mock())
        NodeAgentHandler._write_exception(handler, SandboxRestoreBusyError("busy"))
        call = handler._write_json.call_args
        self.assertEqual(call.kwargs["status"], 503)
        self.assertEqual(call.kwargs["headers"]["Retry-After"], "1")
        self.assertEqual(call.args[0]["error_code"], "node_restore_busy")
        self.assertTrue(call.args[0]["retryable"])

    def test_explicit_wake_preserves_restore_retry_contract(self):
        handler = SimpleNamespace(
            _read_json_body=lambda: {"generation": 7, "operation_id": "wake:test"},
            manager=SimpleNamespace(
                wake_with_activity_revision=Mock(
                    side_effect=SandboxRestoreBusyError("busy")
                ),
                get=Mock(
                    side_effect=AssertionError("must not block on live inspection")
                ),
            ),
            _write_json=Mock(),
        )
        handler._write_exception = lambda exc: NodeAgentHandler._write_exception(
            handler, exc
        )
        NodeAgentHandler._wake_sandbox(handler, "/v1/sandboxes/sandbox/wake")
        self.assertEqual(
            handler._write_json.call_args.args[0]["error_code"], "node_restore_busy"
        )
