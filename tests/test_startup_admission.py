"""Bulk work queues fairly without starving wakes or control requests."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from http.client import HTTPConnection
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock
import time
import unittest
from unittest.mock import patch

from tests import test_control_plane as gateway_fixtures
from tests import test_direct_provisioner as direct_fixtures
from ucloud_sandboxes.admission import FairCapacity
from ucloud_sandboxes.direct_service import DirectSandboxService


def wait_queued(limiter, count):
    deadline = time.monotonic() + 3
    while len(limiter._waiters) != count:
        if time.monotonic() > deadline:
            raise AssertionError("admission did not queue")
        time.sleep(0.005)


class StartupAdmissionTests(unittest.TestCase):
    def test_weighted_fifo_timeout_and_no_barging(self):
        limiter = FairCapacity(10)
        limiter.acquire(weight=6)
        order = []
        release = Event()

        def large():
            self.assertTrue(limiter.acquire(timeout=2, weight=8))
            order.append("large")
            release.wait(2)
            limiter.release(weight=8)

        def small():
            # Both requests may otherwise be admitted by the same release;
            # FIFO reservations do not order concurrently executing threads.
            self.assertTrue(limiter.acquire(timeout=2, weight=3))
            order.append("small")
            limiter.release(weight=3)

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(large)
            wait_queued(limiter, 1)
            self.assertFalse(limiter.acquire(blocking=False))
            self.assertFalse(limiter.acquire(timeout=0.01))
            second = pool.submit(small)
            wait_queued(limiter, 2)
            limiter.release(weight=6)
            release.set()
            first.result(3)
            second.result(3)
        self.assertEqual(order, ["large", "small"])
        self.assertTrue(limiter.acquire(weight=10, blocking=False))
        limiter.release(weight=10)

    def test_create_does_not_block_restore_or_resident_read(self):
        with TemporaryDirectory() as directory:
            fixture = direct_fixtures.DirectProvisionerTests()
            provisioner, *_ = fixture.make(Path(directory).resolve())
            service = DirectSandboxService(
                provisioner,
                max_concurrent_startups=1,
                process_runner=direct_fixtures.FakeProcessRunner(),
            )
            fixture.create(service, fixture.spec())
            service.park("sandbox", operation_id="park:test")
            entered, release = Event(), Event()
            original = provisioner.create

            def slow_create(**kwargs):
                entered.set()
                if not release.wait(3):
                    raise TimeoutError("test did not release create")
                return original(**kwargs)

            with (
                patch.object(provisioner, "create", side_effect=slow_create),
                ThreadPoolExecutor(max_workers=1) as pool,
            ):
                future = pool.submit(
                    fixture.create, service, replace(fixture.spec(), id="cold")
                )
                try:
                    self.assertTrue(entered.wait(3))
                    service.wake("sandbox", generation=7, operation_id="wake:test")
                    self.assertEqual(
                        service.read_file("sandbox", "/marker", max_bytes=1024), b"ok\n"
                    )
                    self.assertFalse(future.done())
                finally:
                    release.set()
                self.assertEqual(future.result(3).state, "running")

    def test_256_requests_queue_and_release_permits_on_failure(self):
        with TemporaryDirectory() as directory:
            fixture = direct_fixtures.DirectProvisionerTests()
            provisioner, *_ = fixture.make(Path(directory).resolve())
            service = DirectSandboxService(provisioner, max_concurrent_startups=8)
            release, full = Event(), Event()
            lock = Lock()
            active = peak = 0

            def work(index):
                nonlocal active, peak
                try:
                    with service.startup_admission():
                        with lock:
                            active += 1
                            peak = max(peak, active)
                            if active == 8:
                                full.set()
                        try:
                            with service.startup_admission():
                                if not release.wait(3):
                                    raise TimeoutError("test gate")
                            if index == 0:
                                raise ValueError("injected failure")
                        finally:
                            with lock:
                                active -= 1
                    return "done"
                except ValueError:
                    return "failed"

            with ThreadPoolExecutor(max_workers=32) as pool:
                futures = [pool.submit(work, i) for i in range(256)]
                try:
                    self.assertTrue(full.wait(3))
                finally:
                    release.set()
                outcomes = [f.result(5) for f in futures]
            self.assertEqual(peak, 8)
            self.assertEqual(outcomes.count("done"), 255)
            self.assertEqual(outcomes.count("failed"), 1)
            self.assertEqual(active, 0)
            self.assertTrue(service._startup_slots.acquire(weight=8, blocking=False))
            service._startup_slots.release(weight=8)

    def test_gateway_queues_create_but_wake_read_and_health_progress(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            gateway = gateway_fixtures._gateway_server(
                root,
                routing_file=root / "routes.sqlite",
                max_concurrent_sandbox_creates=1,
            )
            handler = gateway.RequestHandlerClass

            def created(self):
                self._write_json({"ok": True}, status=201)

            with (
                gateway_fixtures._running_server(gateway),
                patch.object(handler, "_create_sandbox_admitted", created),
                ThreadPoolExecutor(max_workers=1) as pool,
            ):
                host, port = gateway.server_address

                def request(method, path, body=b"{}"):
                    connection = HTTPConnection(host, port, timeout=3)
                    try:
                        connection.request(method, path, body=body)
                        response = connection.getresponse()
                        return response.status, response.read()
                    finally:
                        connection.close()

                limiter = handler.sandbox_create_limiter
                limiter.acquire()
                future = pool.submit(request, "POST", "/v1/sandboxes")
                try:
                    wait_queued(limiter, 1)
                    self.assertFalse(future.done())
                    for method, path, status in [
                        ("POST", "/v1/sandboxes/missing/wake", 404),
                        ("GET", "/v1/sandboxes/missing/files?path=/marker", 404),
                        ("GET", "/healthz", 200),
                        ("DELETE", "/v1/sandboxes/missing", 200),
                    ]:
                        self.assertEqual(request(method, path)[0], status)
                finally:
                    limiter.release()
                self.assertEqual(future.result(3)[0], 201)
                # File routing no longer waits for a whole-body RAM reservation.
                uploads = handler.upload_memory_limiter
                uploads.acquire(weight=uploads.capacity)
                handler.admission_wait_seconds = 0.01
                connection = HTTPConnection(host, port, timeout=2)
                try:
                    connection.putrequest(
                        "PUT", "/v1/sandboxes/missing/files?path=/marker"
                    )
                    connection.putheader("Content-Length", "4096")
                    connection.endheaders()
                    response = connection.getresponse()
                    self.assertEqual(response.status, 404)
                    response.read()
                    self.assertEqual(response.getheader("Connection"), "close")
                finally:
                    connection.close()
                    uploads.release(weight=uploads.capacity)


class WarmDemandTests(unittest.TestCase):
    def test_queued_restore_exposes_demand_and_failure_cleans_it_up(self):
        from ucloud_sandboxes.models import ResourceQuantity
        from ucloud_sandboxes.direct_service import SandboxRestoreBusyError
        with TemporaryDirectory() as directory:
            provisioner, *_ = direct_fixtures.DirectProvisionerTests().make(Path(directory).resolve())
            service = DirectSandboxService(provisioner, max_concurrent_restores=1)
            service.admission_wait_seconds = 0.1
            service._restore_slots.acquire()
            requested = ResourceQuantity(vcpu=1, memory_mb=1024)

            def restore():
                with service._restore_admission('queued', 1, requested):
                    self.fail('restore must not acquire held slot')

            try:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(restore)
                    wait_queued(service._restore_slots, 1)
                    self.assertEqual(service.warm_park_demand_bytes(), 1024**3)
                    with self.assertRaises(SandboxRestoreBusyError):
                        future.result(3)
                self.assertEqual(service.warm_park_demand_bytes(), 0)
                self.assertEqual(service._restore_demands, {})
            finally:
                service._restore_slots.release()

    def test_restore_demand_and_active_reservation_are_not_double_counted(self):
        from ucloud_sandboxes.models import ResourceQuantity
        with TemporaryDirectory() as directory:
            provisioner, *_ = direct_fixtures.DirectProvisionerTests().make(Path(directory).resolve())
            service = DirectSandboxService(provisioner)
            requested = ResourceQuantity(vcpu=1, memory_mb=256)
            service._active_reservations[('same', 1)] = requested
            with service._restore_admission('same', 1, requested):
                self.assertEqual(service.warm_park_demand_bytes(), 256*1024**2)
            self.assertEqual(service._restore_demands, {})
