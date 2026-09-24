from concurrent.futures import ThreadPoolExecutor
from threading import Event, Thread
import time
import unittest

from ucloud_sandboxes.admission import FairRLock


class FairReservationLockTests(unittest.TestCase):
    def wait_queued(self, lock, count):
        deadline = time.monotonic() + 3
        while lock._capacity.waiting != count and time.monotonic() < deadline:
            time.sleep(.001)
        self.assertEqual(lock._capacity.waiting, count)

    def test_burst_is_fifo_and_current_owner_cannot_barge_back_in(self):
        lock = FairRLock()
        order = []
        threads = []
        lock.acquire()
        try:
            for i in range(32):
                def reserve(index=i):
                    with lock:
                        order.append(index)
                thread = Thread(target=reserve)
                thread.start()
                threads.append(thread)
                self.wait_queued(lock, i + 1)
        finally:
            lock.release()
        with lock:
            order.append(32)
        for thread in threads:
            thread.join(3)
            self.assertFalse(thread.is_alive())
        self.assertEqual(order, list(range(33)))

    def test_nested_owner_and_timeout_do_not_lose_capacity(self):
        lock = FairRLock()
        with lock:
            with lock:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    self.assertFalse(pool.submit(lock.acquire, timeout=.01).result())
                self.assertEqual(lock._capacity.waiting, 0)
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertFalse(pool.submit(lock.acquire, blocking=False).result())
        with ThreadPoolExecutor(max_workers=1) as pool:
            def acquire_and_release():
                with lock:
                    return True
            self.assertTrue(pool.submit(acquire_and_release).result(timeout=1))

    def test_foreign_release_is_rejected(self):
        lock = FairRLock()
        with lock, ThreadPoolExecutor(max_workers=1) as pool:
            with self.assertRaises(RuntimeError):
                pool.submit(lock.release).result()
        with self.assertRaises(RuntimeError):
            lock.release()

    def test_create_waits_through_short_contention_without_restarting_request(self):
        from tempfile import TemporaryDirectory
        from pathlib import Path
        from ucloud_sandboxes import control_plane
        from ucloud_sandboxes.models import ResourceQuantity
        from ucloud_sandboxes.routing import RoutingStore

        handler = object.__new__(control_plane.ControlPlaneHandler)
        handler.telemetry = None
        handler.admission_wait_seconds = 2
        entered = Event()
        handler._select_node = lambda *_args, **_kwargs: entered.set()
        with TemporaryDirectory() as directory:
            handler.routing_store = RoutingStore(Path(directory) / 'routes.sqlite')
            with ThreadPoolExecutor(max_workers=1) as pool:
                with control_plane._GATEWAY_SCHEDULING_LOCK:
                    future = pool.submit(
                        handler._select_and_reserve_node, 'test', ResourceQuantity(),
                        spec={'id': 'test'}, spec_hash='a' * 64,
                    )
                    self.wait_queued(control_plane._GATEWAY_SCHEDULING_LOCK, 1)
                    # Previously this aborted at 250 ms and repeated the entire
                    # image-resolution/HTTP pipeline on the next SDK retry.
                    self.assertFalse(entered.wait(.35))
                    self.assertFalse(future.done())
                self.assertIsNone(future.result(timeout=2))
                self.assertTrue(entered.is_set())
