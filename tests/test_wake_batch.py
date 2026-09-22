from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import time
import unittest
from unittest.mock import patch

from tests.test_control_plane import _gateway_server, _sandbox_route, build_heartbeat
from tests import test_wake_capacity as capacity_fixtures
from ucloud_sandboxes import control_plane
from ucloud_sandboxes.models import ResourceQuantity


class WakeBatchTests(unittest.TestCase):
    def fixture(self, root, count, *, device_bound=False):
        server = _gateway_server(root)
        self.addCleanup(server.server_close)
        handler = object.__new__(server.RequestHandlerClass)
        owner = (capacity_fixtures.WakeCapacityTests().heartbeat(active=63) if device_bound else build_heartbeat(
            node_id="node", job_id="job", node_url="http://node:8090",
            capabilities=("sandbox", "disk-quota"),
            total_resources=ResourceQuantity(vcpu=32, memory_mb=98304, disk_mb=1000000),
        ))
        handler.store.upsert_heartbeat(owner)
        routes = []
        for n in range(count):
            route = _sandbox_route(
                sandbox_id=f"local-{n}", node_id="node", job_id="job",
                node_url="http://node:8090", state="parked",
                resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=2048),
                spec={"parkable": True, "managed_process": True},
            )
            routes.append(handler.routing_store.upsert_sandbox(route))
        return handler, routes

    def test_batch_respects_device_capacity_and_deduplicates_wake(self):
        with TemporaryDirectory() as temp:
            handler, routes = self.fixture(Path(temp), 3, device_bound=True)
            with handler._wake_placement_reservation():
                results = handler._reserve_local_wake_batch([
                    (handler, r, None) for r in [routes[0], routes[0], *routes[1:]]
                ])
            self.assertEqual([r.state if r else None for r in results],
                             ["waking", "waking", None, None])
            self.assertEqual([handler.routing_store.get_sandbox(r.sandbox_id).state
                              for r in routes], ["waking", "parked", "parked"])

    def test_waiting_callers_share_commit_and_observe_durable_results(self):
        with TemporaryDirectory() as temp:
            handler, routes = self.fixture(Path(temp), 12)
            batcher = control_plane._LocalWakeBatcher()
            with patch.object(handler.routing_store, "reserve_sandbox_wakes",
                              wraps=handler.routing_store.reserve_sandbox_wakes) as commit:
                with ThreadPoolExecutor(max_workers=len(routes)) as pool:
                    with control_plane._GATEWAY_SCHEDULING_LOCK:
                        futures = [pool.submit(batcher.reserve, handler, r) for r in routes]
                        deadline = time.monotonic() + 5
                        while True:
                            with batcher.lock:
                                queued = len(batcher.pending)
                            if queued == len(routes):
                                break
                            self.assertLess(time.monotonic(), deadline)
                            time.sleep(.001)
                        self.assertFalse(any(f.done() for f in futures))
                    for f in futures:
                        result = f.result(timeout=5)
                        self.assertEqual(handler.routing_store.get_sandbox(result.sandbox_id), result)
                commit.assert_called_once()
                self.assertEqual(len(commit.call_args.args[0]), len(routes))

    def test_failed_commit_rolls_back_every_reservation_and_unblocks_callers(self):
        with TemporaryDirectory() as temp:
            handler, routes = self.fixture(Path(temp), 2)
            store = handler.routing_store
            with store._connect() as conn:
                conn.execute("""CREATE TRIGGER reject_second BEFORE UPDATE ON sandboxes
                    WHEN NEW.sandbox_id = 'local-1' AND NEW.state = 'waking'
                    BEGIN SELECT RAISE(ABORT, 'injected failure'); END""")
            with self.assertRaises(sqlite3.IntegrityError):
                store.reserve_sandbox_wakes([(r, 'wake:' + r.sandbox_id) for r in routes])
            self.assertEqual([store.get_sandbox(r.sandbox_id).state for r in routes],
                             ["parked", "parked"])
            batcher = control_plane._LocalWakeBatcher()
            with self.assertRaises(sqlite3.IntegrityError):
                batcher.reserve(handler, routes[1])
            with store._connect() as conn:
                conn.execute("DROP TRIGGER reject_second")
            self.assertEqual(batcher.reserve(handler, routes[1]).state, "waking")

    def test_batch_rechecks_owner_and_keeps_stale_pending_demand(self):
        with TemporaryDirectory() as temp:
            handler, routes = self.fixture(Path(temp), 2)
            store = handler.routing_store
            stale = replace(routes[1], node_id="different")
            for r in routes:
                store.upsert_pending_with_demand("wake:" + r.sandbox_id, r.resources)
            results = store.reserve_sandbox_wakes([
                (routes[0], "wake:" + routes[0].sandbox_id),
                (stale, "wake:" + stale.sandbox_id),
            ])
            self.assertEqual(results[routes[0].sandbox_id].state, "waking")
            self.assertIsNone(results[stale.sandbox_id])
            with store._connect() as conn:
                pending = {r[0] for r in conn.execute("SELECT sandbox_id FROM pending")}
            self.assertEqual(pending, {"wake:" + stale.sandbox_id})
