from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event, Lock, RLock
import unittest
from unittest.mock import patch

from ucloud_sandboxes.control_plane import ControlPlaneHandler


class FleetReadCoalescingTests(unittest.TestCase):
    def exercise(self, *, fail=False):
        entered, release, joined = Event(), Event(), Event()
        guard = Lock()
        readers = 0
        calls = []

        class ObservedFuture(Future):
            def result(self, *args, **kwargs):
                nonlocal readers
                with guard:
                    readers += 1
                    if readers == 23:
                        joined.set()
                return super().result(*args, **kwargs)

        class Handler(ControlPlaneHandler):
            fleet_response_lock = RLock()
            fleet_response_future = None

            def _sandbox_list_response(self):
                calls.append(True)
                entered.set()
                if not release.wait(3):
                    raise TimeoutError('test did not release fleet scan')
                if fail:
                    raise ValueError('injected scan failure')
                return b'first'

            def _write_bytes(self, body, content_type):
                self.response = (body, content_type)

        def read():
            handler = object.__new__(Handler)
            handler._list_sandboxes_from_cache()
            return handler.response

        with patch('ucloud_sandboxes.control_plane.Future', ObservedFuture):
            with ThreadPoolExecutor(max_workers=24) as pool:
                first = pool.submit(read)
                self.assertTrue(entered.wait(3))
                others = [pool.submit(read) for _ in range(23)]
                try:
                    self.assertTrue(joined.wait(3))
                    self.assertEqual(len(calls), 1)
                finally:
                    release.set()
                for future in [first, *others]:
                    if fail:
                        with self.assertRaisesRegex(ValueError, 'injected scan failure'):
                            future.result(3)
                    else:
                        self.assertEqual(future.result(3), (b'first', 'application/json'))
        # No old snapshot or exception is retained after the concurrent wave.
        self.assertIsNone(Handler.fleet_response_future)
        with patch.object(Handler, '_sandbox_list_response', return_value=b'fresh') as scan:
            self.assertEqual(read(), (b'fresh', 'application/json'))
            scan.assert_called_once()

    def test_concurrent_readers_share_scan_and_encoding_then_refresh(self):
        self.exercise()

    def test_failure_is_shared_and_does_not_poison_next_request(self):
        self.exercise(fail=True)
