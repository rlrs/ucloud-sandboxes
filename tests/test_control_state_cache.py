from dataclasses import replace
from pathlib import Path
import json
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from tests.test_registry import build_heartbeat
from ucloud_sandboxes import control_state
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.models import (
    NODE_RUNTIME_METRIC_DEFAULTS, NodeRuntimeMetrics, SandboxInventoryEntry, utc_now,
)


class ControlStateCacheTests(unittest.TestCase):
    def test_persisted_legacy_metrics_survive_upgrade_and_preserve_validation(self):
        for removed in (("memory_working_set_mb",), tuple(NODE_RUNTIME_METRIC_DEFAULTS)):
            with self.subTest(removed=removed), TemporaryDirectory() as directory:
                path = Path(directory) / "control.sqlite"
                old = ControlStateStore(path)
                old.upsert_heartbeat(replace(self.heartbeat(), runtime_metrics=NodeRuntimeMetrics(collected_at=utc_now())))
                with sqlite3.connect(path) as connection:
                    raw = json.loads(connection.execute("SELECT payload FROM control_records").fetchone()[0])
                    for name in removed:
                        raw["runtime_metrics"].pop(name)
                    payload = control_state._json(raw)
                    connection.execute("UPDATE control_records SET payload = ?", (payload,))
                from scripts.verify_heartbeat_upgrade import verify
                self.assertEqual(verify(path), 1)
                # Open a fresh store as the upgraded gateway/autoscaler does.
                reader = ControlStateStore(path)
                self.assertEqual(reader.load_heartbeats()["job"].runtime_metrics.memory_working_set_mb, 0)
                self.assertTrue(reader.get_heartbeat("job").inventory_complete)
                self.assertFalse(reader.get_heartbeat("job", include_inventory=False).inventory_complete)
                for corrupt in (" " + payload, control_state._json({**raw, "unexpected": True})):
                    with sqlite3.connect(path) as connection:
                        connection.execute("UPDATE control_records SET payload = ?", (corrupt,))
                    with self.assertRaisesRegex(ValueError, "invalid heartbeat"):
                        reader.load_heartbeats()

    def test_nested_additive_metrics_survive_durable_read_and_receive(self):
        from copy import deepcopy
        from ucloud_sandboxes.resource_evidence import DeviceIO, ResourceEvidence
        from ucloud_sandboxes.models import ResidentWaitMetrics
        from scripts.verify_heartbeat_upgrade import verify

        for old_cpu, old_device in ((True, False), (False, True), (True, True)):
            with self.subTest(old_cpu=old_cpu, old_device=old_device), TemporaryDirectory() as directory:
                path = Path(directory) / "control.sqlite"
                heartbeat = replace(self.heartbeat(), received_at=utc_now(), runtime_metrics=NodeRuntimeMetrics(
                    collected_at=utc_now(), resource_evidence=ResourceEvidence(
                        collected_at=utc_now().isoformat(),
                        devices=(DeviceIO(identity="boot:disk", name="vda"),),
                    ),
                    resident_wait=ResidentWaitMetrics(
                        resident_waits=1, checkpoint_inflight=0, checkpoints_completed=0,
                        reclaim_target_bytes=0, projected_reclaim_bytes=0,
                        reason="resident_headroom", admitted_demand_bytes=4096,
                        pending_demand_bytes=2048, unknown_transition_memory_costs=0,
                    ),
                ))
                ControlStateStore(path).receive_heartbeat(heartbeat)
                with sqlite3.connect(path) as connection:
                    current = json.loads(connection.execute("SELECT payload FROM control_records").fetchone()[0])
                    raw = deepcopy(current)
                    evidence = raw["runtime_metrics"]["resource_evidence"]
                    for key in ("admitted_demand_bytes", "pending_demand_bytes",
                                "unknown_transition_memory_costs", "admitted_ram_backing_bytes", "pending_ram_backing_bytes"):
                        raw["runtime_metrics"]["resident_wait"].pop(key)
                    if old_cpu:
                        for key in ("host_cpu_usage_usec", "host_cpu_steal_usec"):
                            evidence.pop(key)
                    if old_device:
                        for key in ("read_bytes", "write_bytes"):
                            evidence["devices"][0].pop(key)
                    payload = control_state._json(raw)
                    connection.execute("UPDATE control_records SET payload=?", (payload,))
                reader = ControlStateStore(path)
                self.assertEqual(verify(path), 1)
                self.assertIsNone(reader.load_heartbeats()["job"].runtime_metrics.resource_evidence.host_cpu_usage_usec)
                self.assertIsNone(reader.load_heartbeats()["job"].runtime_metrics.resident_wait.admitted_demand_bytes)
                self.assertIsNone(reader.get_heartbeat("job", include_inventory=False).runtime_metrics.resource_evidence.devices[0].write_bytes)
                reader.receive_heartbeat(replace(heartbeat, activity_epoch=heartbeat.activity_epoch + 1, received_at=utc_now()))
                self.assertEqual(reader.load_heartbeats()["job"].activity_epoch, heartbeat.activity_epoch + 1)
                with sqlite3.connect(path) as connection:
                    refreshed = json.loads(connection.execute("SELECT payload FROM control_records").fetchone()[0])
                self.assertIn("host_cpu_usage_usec", refreshed["runtime_metrics"]["resource_evidence"])
                self.assertEqual(refreshed["runtime_metrics"]["resident_wait"]["admitted_demand_bytes"], 4096)
                self.assertIn("read_bytes", refreshed["runtime_metrics"]["resource_evidence"]["devices"][0])
                for field, value in (("unexpected", None), ("memory_dirty_bytes", -1), ("collected_at", None)):
                    corrupt = deepcopy(raw)
                    corrupt["runtime_metrics"]["resource_evidence"][field] = value
                    with sqlite3.connect(path) as connection:
                        connection.execute("UPDATE control_records SET payload=?", (control_state._json(corrupt),))
                    for read in (reader.load_heartbeats, lambda: reader.get_heartbeat("job", include_inventory=False)):
                        with self.assertRaisesRegex(ValueError, "invalid heartbeat"):
                            read()
                with sqlite3.connect(path) as connection:
                    connection.execute("UPDATE control_records SET payload=?", (" " + payload,))
                with self.assertRaisesRegex(ValueError, "invalid heartbeat"):
                    reader.load_heartbeats()

    def test_header_read_omits_inventory_but_rechecks_external_authority(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "control.sqlite"
            reader, writer = ControlStateStore(path), ControlStateStore(path)
            writer.receive_heartbeat(replace(self.heartbeat(), received_at=utc_now()))
            header = reader.get_heartbeat("job", include_inventory=False)
            self.assertEqual(header.inventory, ())
            self.assertFalse(header.inventory_complete)
            self.assertEqual(header.node_epoch, "boot-1")
            header.labels["pool"] = "changed"
            full = reader.get_heartbeat("job")
            self.assertTrue(full.inventory_complete)
            self.assertEqual(len(full.inventory), 1)
            self.assertEqual(full.labels["pool"], "workers")
            writer.quarantine_node("job", "test")
            self.assertFalse(reader.get_heartbeat("job", include_inventory=False).admission_open)
            writer.receive_heartbeat(replace(
                self.heartbeat(), node_epoch="boot-2", received_at=utc_now(),
            ))
            self.assertEqual(reader.get_heartbeat("job", include_inventory=False).node_epoch, "boot-2")
            with sqlite3.connect(path) as connection:
                connection.execute("UPDATE control_records SET payload = ' ' || payload")
            with self.assertRaisesRegex(ValueError, "invalid heartbeat"):
                reader.get_heartbeat("job", include_inventory=False)
            with sqlite3.connect(path) as connection:
                connection.execute("DELETE FROM control_records")
            self.assertIsNone(reader.get_heartbeat("job", include_inventory=False))

    def heartbeat(self, job_id="job"):
        return build_heartbeat(
            job_id=job_id, node_id=f"node-{job_id}",
            node_url=f"http://node-{job_id}:8090", node_epoch="boot-1",
            labels={"pool": "workers"}, inventory_complete=True,
            inventory=(SandboxInventoryEntry(
                sandbox_id="sandbox", generation=1, operation_id="create-1",
                spec_hash="a" * 64, state="running",
                storage_dependency={"layers": [{"digest": "original"}]},
            ),),
        )

    def test_shared_fleet_read_reuses_cached_objects_until_payload_changes(self):
        with TemporaryDirectory() as directory:
            store = ControlStateStore(Path(directory) / "control.sqlite")
            store.upsert_heartbeat(self.heartbeat())
            first = store.load_heartbeats(shared=True)["job"]
            self.assertIs(store.load_heartbeats(shared=True)["job"], first)
            # The default read still isolates callers from the cached object.
            copied = store.load_heartbeats()["job"]
            self.assertIsNot(copied, first)
            self.assertEqual(copied, first)
            self.assertIsNot(
                copied.inventory[0].storage_dependency,
                first.inventory[0].storage_dependency,
            )
            detached = control_state.detached_heartbeat(first)
            detached.labels["pool"] = "changed"
            self.assertEqual(first.labels["pool"], "workers")
            store.upsert_heartbeat(replace(self.heartbeat(), node_epoch="boot-2"))
            refreshed = store.load_heartbeats(shared=True)["job"]
            self.assertIsNot(refreshed, first)
            self.assertEqual(refreshed.node_epoch, "boot-2")

    def test_reuses_decode_but_returns_isolated_mutable_inventory(self):
        with TemporaryDirectory() as directory:
            store = ControlStateStore(Path(directory) / "control.sqlite")
            store.upsert_heartbeat(self.heartbeat())
            with patch.object(store, "_decode_heartbeat", wraps=store._decode_heartbeat) as decode:
                first = store.get_heartbeat("job")
                first.labels["pool"] = "changed"
                first.inventory[0].storage_dependency["layers"][0]["digest"] = "changed"
                first.inventory[0].storage_snapshot["unexpected"] = True
                second = store.load_heartbeats()["job"]
                third = store.get_heartbeat("job")
                self.assertEqual(decode.call_count, 1)
            for heartbeat in (second, third):
                self.assertEqual(heartbeat.labels, {"pool": "workers"})
                self.assertEqual(heartbeat.inventory[0].storage_snapshot, {})
                self.assertEqual(heartbeat.inventory[0].storage_dependency,
                                 {"layers": [{"digest": "original"}]})

    def test_other_connection_quarantine_reboot_delete_and_corruption_are_immediate(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "control.sqlite"
            reader, writer = ControlStateStore(path), ControlStateStore(path)
            heartbeat = replace(self.heartbeat(), received_at=utc_now())
            writer.receive_heartbeat(heartbeat)
            self.assertTrue(reader.get_heartbeat("job").admission_open)
            writer.quarantine_node("job", "test")
            self.assertFalse(reader.get_heartbeat("job").admission_open)
            writer.receive_heartbeat(replace(
                heartbeat, node_epoch="boot-2", received_at=utc_now(),
            ))
            current = reader.get_heartbeat("job")
            self.assertEqual(current.node_epoch, "boot-2")
            self.assertEqual(current.retired_node_epochs, ("boot-1",))
            self.assertFalse(current.admission_open)
            with sqlite3.connect(path) as connection:
                connection.execute("UPDATE control_records SET payload = ' ' || payload")
            for load in (lambda: reader.get_heartbeat("job"), reader.load_heartbeats):
                with self.assertRaisesRegex(ValueError, "invalid heartbeat"):
                    load()
            with sqlite3.connect(path) as connection:
                connection.execute("DELETE FROM control_records")
            self.assertIsNone(reader.get_heartbeat("job"))
            self.assertEqual(reader.load_heartbeats(), {})

    def test_eviction_and_oversized_rows_do_not_limit_fleet_reads(self):
        with TemporaryDirectory() as directory:
            store = ControlStateStore(Path(directory) / "control.sqlite")
            with patch.object(control_state, "_HEARTBEAT_CACHE_ENTRIES", 2):
                for index in range(4):
                    store.upsert_heartbeat(self.heartbeat(str(index)))
                self.assertEqual(len(store.load_heartbeats()), 4)
                self.assertLessEqual(len(store._heartbeat_cache), 2)
            # Force byte eviction separately from entry eviction.
            with patch.object(control_state, "_HEARTBEAT_CACHE_BYTES", 1):
                store.upsert_heartbeat(self.heartbeat("large"))
                self.assertEqual(store.get_heartbeat("large").node_id, "node-large")
                self.assertNotIn("large", store._heartbeat_cache)
            self.assertEqual(len(store.load_heartbeats()), 5)

    def test_file_identity_check_does_not_block_returning_another_reader(self):
        from threading import Event, Thread, current_thread
        with TemporaryDirectory() as raw:
            store = ControlStateStore(Path(raw) / 'control.sqlite')
            entered, release, returned = Event(), Event(), Event()
            original = Path.stat
            errors = []
            def slow_stat(path, *args, **kwargs):
                if path == store.path and current_thread().name == 'slow-reader':
                    entered.set()
                    release.wait(2)
                return original(path, *args, **kwargs)
            held = store._connection()
            held.__enter__()
            def read():
                try:
                    store.load_heartbeats()
                except BaseException as exc:
                    errors.append(exc)
            def give_back():
                try:
                    held.__exit__(None, None, None)
                finally:
                    returned.set()
            with patch.object(Path, 'stat', slow_stat):
                reader = Thread(target=read, name='slow-reader')
                reader.start()
                self.assertTrue(entered.wait(2))
                returning = Thread(target=give_back)
                returning.start()
                try:
                    self.assertTrue(returned.wait(.5), 'identity stat held the pool mutex')
                finally:
                    release.set()
                    reader.join(3)
                    returning.join(3)
            self.assertFalse(errors)

    def test_exec_absence_check_only_loads_inventory_for_an_empty_worker(self):
        from ucloud_sandboxes.exec_routing import ExecRoutingService, heartbeat_proves_route_absent
        from ucloud_sandboxes.routing import ExecRoute
        with TemporaryDirectory() as raw:
            store = ControlStateStore(Path(raw) / 'control.sqlite')
            heartbeat = replace(self.heartbeat(), received_at=utc_now(), active_sandboxes=1)
            store.receive_heartbeat(heartbeat)
            routing = ExecRoutingService(store, None, 120)
            route = ExecRoute(session_id='exec', sandbox_id='sandbox', node_id=heartbeat.node_id,
                              job_id=heartbeat.job_id, node_url=heartbeat.node_url,
                              created_at='2026-01-01T00:00:00+00:00', updated_at='2026-01-01T00:00:00+00:00')
            def stale():
                return heartbeat_proves_route_absent(
                    routing.heartbeat(route), sandbox_id=route.sandbox_id,
                    route_created_at=route.created_at, route_updated_at=route.updated_at,
                    heartbeat_ttl_seconds=routing.heartbeat_ttl_seconds,
                )
            with patch.object(store, 'get_heartbeat', wraps=store.get_heartbeat) as get:
                self.assertFalse(stale())
                get.assert_called_once_with('job', include_inventory=False)
            # Zero active count does not mean the parked sandbox disappeared.
            store.receive_heartbeat(replace(heartbeat, active_sandboxes=0, received_at=utc_now()))
            self.assertFalse(stale())
            store.receive_heartbeat(replace(heartbeat, active_sandboxes=0, inventory=(), received_at=utc_now()))
            self.assertTrue(stale())

    def test_single_heartbeat_read_has_no_explicit_transaction_and_keeps_permissions(self):
        with TemporaryDirectory() as directory:
            store = ControlStateStore(Path(directory) / 'control.sqlite')
            store.upsert_heartbeat(self.heartbeat())
            statements = []
            with store._connection() as conn:
                conn.set_trace_callback(statements.append)
            store.path.chmod(0o644)
            store._connection_checked_at = float('-inf')  # file checks run once a second
            self.assertEqual(store.get_heartbeat('job').job_id, 'job')
            self.assertFalse(any(s.startswith(('BEGIN', 'COMMIT')) for s in statements))
            self.assertEqual(store.path.stat().st_mode & 0o777, 0o600)
            for suffix in ('-wal', '-shm'):
                path = Path(str(store.path) + suffix)
                if path.exists():
                    self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_body_pooling_requires_fresh_capability_on_the_observed_origin(self):
        from datetime import timedelta
        from ucloud_sandboxes.capabilities import REQUEST_BODY_KEEPALIVE_CAPABILITY
        from ucloud_sandboxes.control_plane import ControlPlaneHandler
        with TemporaryDirectory() as directory:
            store = ControlStateStore(Path(directory) / 'control.sqlite')
            handler = object.__new__(ControlPlaneHandler)
            handler.store, handler.heartbeat_ttl_seconds = store, 30
            for capable, age, expected in [(False, 0, None), (True, 0, 'http://node-job:8090'), (True, 60, None)]:
                observed = utc_now() - timedelta(seconds=age)
                store.upsert_heartbeat(replace(self.heartbeat(), received_at=observed,
                    capabilities=(REQUEST_BODY_KEEPALIVE_CAPABILITY,) if capable else ()))
                handler._heartbeat_for_route(job_id='job', include_inventory=False)
                self.assertEqual(handler._pooled_node_body_origin, expected)
            handler._heartbeat_for_route(job_id='missing')
            self.assertIsNone(handler._pooled_node_body_origin)
