from concurrent.futures import ThreadPoolExecutor
from threading import Event
import unittest
from unittest.mock import patch

from tests.test_startup_admission import wait_queued
from ucloud_sandboxes.admission import FairCapacity, _Waiter


class FairCapacityTests(unittest.TestCase):
    def test_release_only_wakes_granted_head(self):
        capacity = FairCapacity(1)
        capacity.acquire()
        done = Event()
        entered = Event()

        def run():
            self.assertTrue(capacity.acquire(timeout=3))
            entered.set()
            try:
                self.assertTrue(done.wait(3))
            finally:
                capacity.release()

        with ThreadPoolExecutor(max_workers=32) as pool:
            futures = [pool.submit(run) for _ in range(32)]
            try:
                wait_queued(capacity, 32)
                tickets = list(capacity._waiters)
                capacity.release()
                self.assertTrue(entered.wait(3))
                self.assertTrue(tickets[0].ready.is_set())
                self.assertTrue(all(not t.ready.is_set() for t in tickets[1:]))
                self.assertFalse(capacity.acquire(blocking=False))
            finally:
                done.set()
            for future in futures:
                future.result(3)
        self.assertTrue(capacity.acquire(blocking=False))
        capacity.release()

    def test_timed_out_weighted_head_grants_fitting_follower(self):
        capacity = FairCapacity(10)
        capacity.acquire(weight=6)
        with ThreadPoolExecutor(max_workers=2) as pool:
            head = pool.submit(capacity.acquire, timeout=.2, weight=8)
            wait_queued(capacity, 1)
            follower = pool.submit(capacity.acquire, timeout=2, weight=4)
            self.assertFalse(head.result(3))
            self.assertTrue(follower.result(3))
        capacity.release(weight=4)
        capacity.release(weight=6)
        self.assertTrue(capacity.acquire(weight=10, blocking=False))

    def test_grant_racing_timeout_is_not_lost(self):
        capacity = FairCapacity(1)
        capacity.acquire()
        ticket = _Waiter(1)

        def racing_wait(timeout):
            capacity.release()
            return False

        with patch('ucloud_sandboxes.admission._Waiter', return_value=ticket), patch.object(ticket.ready, 'wait', side_effect=racing_wait):
            self.assertTrue(capacity.acquire(timeout=0))
        self.assertFalse(capacity.acquire(blocking=False))
        capacity.release()
        self.assertEqual(capacity._available, 1)

    def test_interruption_returns_an_already_granted_reservation(self):
        for grant in (False, True):
            with self.subTest(grant=grant):
                capacity = FairCapacity(1)
                capacity.acquire()
                ticket = _Waiter(1)

                def interrupted_wait(timeout):
                    if grant:
                        capacity.release()
                    raise KeyboardInterrupt

                with patch('ucloud_sandboxes.admission._Waiter', return_value=ticket), patch.object(ticket.ready, 'wait', side_effect=interrupted_wait):
                    with self.assertRaises(KeyboardInterrupt):
                        capacity.acquire(timeout=3)
                if not grant:
                    capacity.release()
                self.assertFalse(capacity._waiters)
                self.assertTrue(capacity.acquire(blocking=False))
