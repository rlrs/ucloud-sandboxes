"""Cold-start work must yield capacity without blocking control traffic."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from http.client import HTTPConnection
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock
from types import SimpleNamespace
from urllib.parse import urlparse
import json
import unittest
from unittest.mock import Mock, patch

from tests import test_control_plane as gateway_fixtures
from tests import test_direct_provisioner as direct_fixtures
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.node_agent import NodeAgentHandler
from ucloud_sandboxes.sandbox import SandboxStartupBusyError


class StartupAdmissionTests(unittest.TestCase):
    def test_create_restore_and_upload_share_budget_without_blocking_inventory(self):
        with TemporaryDirectory() as directory:
            fixture = direct_fixtures.DirectProvisionerTests()
            provisioner, *_ = fixture.make(Path(directory).resolve())
            service = DirectSandboxService(provisioner, max_concurrent_startups=1)
            fixture.create(service, fixture.spec())
            service.park("sandbox", operation_id="park:test")
            entered, release = Event(), Event()
            original = provisioner.create

            def slow_create(**kwargs):
                entered.set()
                if not release.wait(5):
                    raise TimeoutError("test did not release create")
                return original(**kwargs)

            with patch.object(provisioner, "create", side_effect=slow_create):
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(
                        fixture.create, service, replace(fixture.spec(), id="cold")
                    )
                    try:
                        self.assertTrue(entered.wait(5))
                        with self.assertRaises(SandboxStartupBusyError):
                            service.wake(
                                "sandbox", generation=7, operation_id="wake:test"
                            )
                        with self.assertRaises(SandboxStartupBusyError):
                            service.write_file("sandbox", "/tmp/test", b"data")
                        self.assertEqual(
                            service.get_snapshot("sandbox").state, "parked"
                        )
                        handler = SimpleNamespace(
                            manager=SimpleNamespace(
                                service=service,
                                upload_file=Mock(side_effect=AssertionError),
                            ),
                            _read_raw_body=Mock(
                                side_effect=AssertionError(
                                    "must reject before body buffering"
                                )
                            ),
                            _write_json=Mock(),
                        )
                        handler._write_exception = lambda exc: (
                            NodeAgentHandler._write_exception(handler, exc)
                        )
                        NodeAgentHandler._upload_file(
                            handler,
                            urlparse("/v1/sandboxes/sandbox/files?path=/tmp/test"),
                        )
                        self.assertEqual(
                            handler._write_json.call_args.args[0]["error_code"],
                            "node_startup_busy",
                        )
                    finally:
                        release.set()
                    self.assertEqual(future.result(timeout=5).state, "running")
            # An admitted upload can restore without needing a second permit.
            with service.startup_admission():
                service.wake("sandbox", generation=7, operation_id="wake:retry")
            self.assertEqual(service.get_snapshot("sandbox").state, "running")
            # Same-sandbox contention is also rejected before command execution.
            with service._lock("sandbox", 7):
                with self.assertRaises(SandboxStartupBusyError):
                    service.write_file("sandbox", "/tmp/test", b"data")
            with service.startup_admission():
                pass

    def test_256_request_burst_bounds_work_and_releases_permits(self):
        with TemporaryDirectory() as directory:
            fixture = direct_fixtures.DirectProvisionerTests()
            provisioner, *_ = fixture.make(Path(directory).resolve())
            service = DirectSandboxService(provisioner, max_concurrent_startups=8)
            release, rejected_all = Event(), Event()
            lock = Lock()
            active = peak = rejected = 0

            def work(index):
                nonlocal active, peak, rejected
                try:
                    with service.startup_admission():
                        with lock:
                            active += 1
                            peak = max(peak, active)
                        try:
                            with service.startup_admission():
                                if not release.wait(5):
                                    raise TimeoutError("test did not release work")
                            if index == 0:
                                raise RuntimeError("injected operation failure")
                        finally:
                            with lock:
                                active -= 1
                    return "done"
                except SandboxStartupBusyError:
                    with lock:
                        rejected += 1
                        if rejected == 248:
                            rejected_all.set()
                    return "deferred"
                except RuntimeError:
                    return "failed"

            with ThreadPoolExecutor(max_workers=32) as pool:
                futures = [pool.submit(work, index) for index in range(256)]
                try:
                    self.assertTrue(rejected_all.wait(5))
                    self.assertEqual(peak, 8)
                finally:
                    release.set()
                outcomes = [future.result(timeout=5) for future in futures]
            self.assertEqual(outcomes.count("deferred"), 248)
            self.assertEqual(active, 0)
            # Every successful/failed request releases its permit.
            self.assertTrue(
                all(service._startup_slots.acquire(blocking=False) for _ in range(8))
            )
            for _ in range(8):
                service._startup_slots.release()

    def test_gateway_rejects_unread_upload_but_serves_health_and_delete(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            gateway = gateway_fixtures._gateway_server(
                root,
                routing_file=root / "routes.sqlite",
                max_concurrent_sandbox_creates=1,
            )
            with gateway_fixtures._running_server(gateway):
                host, port = gateway.server_address
                limiter = gateway.RequestHandlerClass.sandbox_create_limiter
                limiter.acquire()
                try:
                    for method, path in (
                        ("PUT", "/v1/sandboxes/sandbox/files?path=/tmp/test"),
                        ("POST", "/v1/sandboxes"),
                        ("POST", "/v1/sandboxes/sandbox/wake"),
                    ):
                        connection = HTTPConnection(host, port, timeout=2)
                        try:
                            connection.putrequest(method, path)
                            connection.putheader("Content-Length", "4096")
                            connection.endheaders()  # Deliberately send no body.
                            response = connection.getresponse()
                            self.assertEqual(response.status, 503)
                            self.assertEqual(
                                json.loads(response.read())["error_code"],
                                "gateway_startup_busy",
                            )
                            self.assertEqual(response.getheader("Connection"), "close")
                        finally:
                            connection.close()
                    for method, path in (
                        ("GET", "/healthz"),
                        ("DELETE", "/v1/sandboxes/missing"),
                    ):
                        connection = HTTPConnection(host, port, timeout=2)
                        try:
                            connection.request(method, path)
                            response = connection.getresponse()
                            self.assertEqual(response.status, 200)
                            response.read()
                        finally:
                            connection.close()
                finally:
                    limiter.release()
