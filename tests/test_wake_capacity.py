from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from types import SimpleNamespace
from unittest.mock import Mock, patch
import unittest

from tests import test_control_plane as fixtures
from tests import test_direct_provisioner as direct_fixtures
from ucloud_sandboxes import control_plane
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.registry import heartbeat_to_dict
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, SandboxInventoryEntry, utc_now
from ucloud_sandboxes.routing import RoutingStore, wake_pending_demand_id
from ucloud_sandboxes.wake_placement import WakeCapacityRefreshPending, WakeSnapshotPublicationRequired
from ucloud_sandboxes.node_agent import NodeAgentHandler
from ucloud_sandboxes.sandbox import SandboxCapacityUnavailableError


class WakeCapacityTests(unittest.TestCase):
    def test_capacity_failure_is_retryable_only_when_wake_is_still_parked(self):
        for state in ("parked", "running", "restoring"):
            with self.subTest(state=state):
                handler = object.__new__(NodeAgentHandler)
                handler._read_json_body = lambda: {"generation": 1, "operation_id": "wake:test"}
                handler.manager = Mock()
                handler.manager.wake_with_activity_revision.side_effect = SandboxCapacityUnavailableError("device capacity exhausted")
                handler.manager.get.return_value = SimpleNamespace(state=state)
                handler._write_json = Mock()
                handler._wake_sandbox("/v1/sandboxes/parked/wake")
                payload = handler._write_json.call_args.args[0]
                if state == "parked":
                    self.assertEqual(payload["error_code"], "node_restore_busy")
                    self.assertTrue(payload["retryable"])
                else:
                    self.assertNotIn("retryable", payload)

    def heartbeat(self, *, active=63):
        return fixtures.build_heartbeat(
            node_id="node", job_id="job", node_url="http://node:8090",
            capabilities=("sandbox", "disk-quota", "storage-native-v1"),
            total_resources=ResourceQuantity(vcpu=32, memory_mb=98304, disk_mb=1000000),
            runtime_metrics=NodeRuntimeMetrics(
                collected_at=utc_now(), cpu_percent=0, cpu_count=32,
                memory_total_mb=98304, memory_available_mb=98304,
                storage_hard_capacity_mb=1000000,
                storage_ublk_active_devices=active, storage_ublk_max_devices=64,
            ),
            inventory_complete=True,
        )

    def route(self, state="parked"):
        return fixtures._sandbox_route(
            sandbox_id="parked", node_id="node", job_id="job",
            node_url="http://node:8090", state=state,
            resources=ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=5184),
            spec={"id": "parked", "image": "busybox"},
        )

    def test_wakes_reserve_devices_until_observed_live(self):
        route = self.route("waking")
        entry = SandboxInventoryEntry(
            sandbox_id=route.sandbox_id, generation=route.generation,
            operation_id=route.create_operation_id, spec_hash=route.spec_hash,
            state="parked", resources=route.resources,
        )
        heartbeat = replace(self.heartbeat(), inventory=(entry,))
        self.assertFalse(control_plane._node_has_storage_device_capacity(heartbeat, [route]))
        self.assertEqual(control_plane._node_reserved_storage_device_slots(heartbeat, [route, route]), 1)
        running = replace(route, state="running")
        self.assertFalse(control_plane._node_has_storage_device_capacity(heartbeat, [running]))
        self.assertEqual(control_plane._node_reserved_storage_device_slots(heartbeat, [running]), 1)
        self.assertTrue(control_plane._node_has_storage_device_capacity(heartbeat, [replace(route, state="parked")]))
        observed = replace(heartbeat, inventory=(replace(entry, state="restoring"),))
        self.assertEqual(control_plane._node_reserved_storage_device_slots(observed, [route]), 0)
        self.assertFalse(control_plane._node_has_storage_device_capacity(self.heartbeat(active=64), []))

    def test_full_owner_requests_publication_outside_placement_lock(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = RoutingStore(root / "routes.sqlite")
            handler.store = ControlStateStore(root / "control-state.sqlite")
            handler.heartbeat_ttl_seconds = 120
            source = replace(self.heartbeat(active=64), capabilities=(
                *self.heartbeat().capabilities, "sandbox-migrate-storage-native-v1",
            ))
            handler.store.upsert_heartbeat(source)
            handler.store.upsert_heartbeat(replace(
                source, node_id="destination", job_id="destination-job", node_url="http://dest:8090",
                runtime_metrics=replace(source.runtime_metrics, storage_ublk_active_devices=0),
            ))
            route = handler.routing_store.upsert_sandbox(self.route())
            handler._write_json = Mock()
            handler._refresh_wake_capacity = Mock(return_value=False)
            acquired = []

            def queue_publication(url, path, **kwargs):
                def check_lock():
                    ok = control_plane._GATEWAY_SCHEDULING_LOCK.acquire(timeout=0.2)
                    acquired.append(ok)
                    if ok:
                        control_plane._GATEWAY_SCHEDULING_LOCK.release()
                thread = Thread(target=check_lock)
                thread.start()
                thread.join(timeout=1)
                self.assertEqual(path, "/v1/sandboxes/parked/snapshot/publish")
                self.assertEqual(kwargs["method"], "POST")
                return control_plane.ProxiedResponse(202, {}, b"{}")

            handler._proxy_request = queue_publication
            self.assertIsNone(fixtures._prepare_wake_route(handler, route))
            self.assertEqual(acquired, [True])
            self.assertEqual(handler._write_json.call_args.args[0]["error_code"], "snapshot_publication_pending")
            self.assertEqual(handler.routing_store.get_sandbox_readonly("parked").state, "parked")

    def test_full_fleet_defers_export_but_keeps_wake_demand(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = RoutingStore(root / "routes.sqlite")
            handler.store = ControlStateStore(root / "control-state.sqlite")
            handler.heartbeat_ttl_seconds = 120
            source = replace(self.heartbeat(active=64), capabilities=(
                *self.heartbeat().capabilities, "sandbox-migrate-storage-native-v1",
            ))
            handler.store.upsert_heartbeat(source)
            destination = replace(source, node_id="destination", job_id="dest-job", node_url="http://dest:8090")
            handler.store.upsert_heartbeat(destination)
            route = handler.routing_store.upsert_sandbox(self.route())
            handler._write_json = Mock()
            handler._refresh_wake_capacity = Mock(return_value=False)
            handler._proxy_request = Mock(return_value=control_plane.ProxiedResponse(202, {}, b"{}"))
            self.assertIsNone(fixtures._prepare_wake_route(handler, route))
            handler._proxy_request.assert_not_called()
            self.assertEqual(handler._write_json.call_args.args[0]["error_code"], "wake_destination_unavailable")
            pending = handler.routing_store.get_pending(wake_pending_demand_id(route.sandbox_id))
            self.assertEqual(pending.resources, route.resources)
            self.assertEqual(pending.failure_reason, "wake_destination_unavailable")
            handler.store.upsert_heartbeat(replace(destination, runtime_metrics=replace(
                destination.runtime_metrics, storage_ublk_active_devices=0,
            )))
            self.assertIsNone(fixtures._prepare_wake_route(handler, route))
            self.assertEqual(handler._proxy_request.call_count, 1)
            self.assertTrue(handler._proxy_request.call_args.args[1].endswith("/snapshot/publish"))

    def test_closed_source_can_offload_but_closed_destination_cannot_admit(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = RoutingStore(root / "routes.sqlite")
            handler.store = ControlStateStore(root / "control-state.sqlite")
            handler.heartbeat_ttl_seconds = 120
            source = replace(self.heartbeat(active=64), admission_open=False, draining=True, capabilities=(
                *self.heartbeat().capabilities, "sandbox-migrate-storage-native-v1",
            ))
            handler.store.upsert_heartbeat(source)
            destination = replace(
                source, node_id="destination", job_id="dest-job", node_url="http://dest:8090",
                admission_open=True, draining=False,
                runtime_metrics=replace(source.runtime_metrics, storage_ublk_active_devices=0),
            )
            handler.store.upsert_heartbeat(destination)
            route = handler.routing_store.upsert_sandbox(self.route())
            with patch.object(control_plane, '_node_available_resources',
                              wraps=control_plane._node_available_resources) as available:
                selected = handler._select_migration_destination(
                    route, requested_node_id="", require_active_resources=True,
                )
            self.assertEqual(available.call_count, 1)
            self.assertEqual(available.call_args.args[1], [])
            self.assertEqual(selected.node_id, "destination")
            # Even if runtime metrics look healthy, a closed admission gate
            # must not be bypassed by reserving a local wake.
            handler.store.upsert_heartbeat(replace(
                source, draining=False, runtime_metrics=destination.runtime_metrics,
            ))
            with self.assertRaises(WakeSnapshotPublicationRequired):
                handler._wake_placement().reserve(route)
            self.assertEqual(handler.routing_store.get_sandbox(route.sandbox_id).state, "parked")
            handler.store.upsert_heartbeat(replace(destination, admission_open=False))
            self.assertIsNone(handler._select_migration_destination(route, requested_node_id="", require_active_resources=True))

    def test_fresh_capacity_avoids_publication_after_a_full_worker_parks(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = RoutingStore(root / "routes.sqlite")
            handler.store = ControlStateStore(root / "control-state.sqlite")
            handler.heartbeat_ttl_seconds = 120
            heartbeat = self.heartbeat(active=64)
            handler.store.upsert_heartbeat(heartbeat)
            route = handler.routing_store.upsert_sandbox(self.route())
            refreshed = replace(heartbeat, runtime_metrics=replace(
                heartbeat.runtime_metrics, storage_ublk_active_devices=0,
            ))
            acquired = []
            def refresh(url, path, **kwargs):
                def check_lock():
                    ok = control_plane._GATEWAY_SCHEDULING_LOCK.acquire(timeout=.2)
                    acquired.append(ok)
                    if ok:
                        control_plane._GATEWAY_SCHEDULING_LOCK.release()
                thread = Thread(target=check_lock)
                thread.start()
                thread.join(1)
                self.assertEqual(path, "/v1/heartbeat")
                # Concurrent callers defer rather than fetching or publishing.
                with self.assertRaises(WakeCapacityRefreshPending):
                    handler._refresh_wake_capacity(route)
                import json
                return control_plane.ProxiedResponse(200, {}, json.dumps({
                    "heartbeat": heartbeat_to_dict(refreshed),
                }).encode())
            handler._proxy_request = Mock(side_effect=refresh)
            self.assertEqual(fixtures._prepare_wake_route(handler, route).state, "waking")
            self.assertEqual(acquired, [True])
            self.assertEqual(handler._proxy_request.call_count, 1)
            self.assertEqual(handler.store.load_heartbeats()["job"].runtime_metrics.storage_ublk_active_devices, 0)
            handler.store.upsert_heartbeat(heartbeat)
            self.assertFalse(handler._refresh_wake_capacity(route))
            self.assertEqual(handler._proxy_request.call_count, 1)

    def test_unblocked_wake_reads_owner_inventory_once_without_capacity_refresh(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            server = fixtures._gateway_server(root)
            try:
                handler = object.__new__(server.RequestHandlerClass)
                handler.store.upsert_heartbeat(self.heartbeat(active=0))
                route = handler.routing_store.upsert_sandbox(self.route())
                handler._write_json = Mock()
                with (
                    patch.object(handler, '_refresh_wake_capacity', side_effect=AssertionError('redundant refresh')),
                    patch.object(handler, '_placement_routes_for_node', wraps=handler._placement_routes_for_node) as inventory,
                ):
                    result = fixtures._prepare_wake_route(handler, route)
                self.assertEqual(result.state, 'waking')
                self.assertEqual(inventory.call_count, 1)
            finally:
                server.server_close()

    def test_completed_publication_is_used_without_waiting_for_heartbeat(self):
        with TemporaryDirectory() as directory:
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = RoutingStore(Path(directory) / "routes.sqlite")
            handler.store = ControlStateStore(Path(directory) / "control.sqlite")
            handler.heartbeat_ttl_seconds = 120
            snapshot = fixtures._portable_snapshot("parked")
            spec = snapshot.manifest.spec
            route = handler.routing_store.upsert_sandbox(replace(
                self.route(), spec=spec.to_dict(), spec_hash=snapshot.manifest.spec_sha256,
                resources=spec.requested_resources(),
            ))
            record = {
                "spec": spec.to_dict(), "state": "parked", "generation": route.generation,
                "operation_id": route.create_operation_id, "spec_hash": route.spec_hash,
                "storage_schema": "storage-native-v1", "storage_snapshot": snapshot.to_dict(),
                "snapshot_manifest_digest": snapshot.publication.manifest_digest,
                "snapshot_repository": snapshot.publication.repository,
                "snapshot_tag": snapshot.publication.tag, "snapshot_sha256": snapshot.sha256,
            }
            refreshed = handler._wake_placement().accept_publication(route, {"sandbox": record})
            self.assertIsNotNone(refreshed)
            self.assertTrue(control_plane.is_portable_parked_route(refreshed))
            handler.routing_store.upsert_sandbox(replace(refreshed, state="running"))
            self.assertIsNone(handler._wake_placement().accept_publication(route, {"sandbox": record}))
            self.assertEqual(handler.routing_store.get_sandbox_readonly("parked").state, "running")

    def test_migration_reserves_last_destination_device_slot(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = RoutingStore(root / "routes.sqlite")
            source = handler.routing_store.upsert_sandbox(self.route())
            another = handler.routing_store.upsert_sandbox(fixtures._sandbox_route(
                sandbox_id="other", node_id="node", job_id="job", node_url="http://node:8090",
                state="parked", resources=source.resources, spec={"id": "other", "image": "busybox"},
            ))
            capabilities = self.heartbeat().capabilities + ("sandbox-migrate-storage-native-v1",)
            source_node = replace(self.heartbeat(), capabilities=capabilities)
            destination = replace(source_node, node_id="destination", job_id="dest-job", node_url="http://dest:8090")
            handler._ready_sandbox_heartbeats = lambda **_kwargs: [source_node, destination]
            # Parked owners themselves need no new device until a wake is reserved.
            self.assertTrue(control_plane._node_has_storage_device_capacity(source_node, [source]))
            self.assertIsNotNone(handler._select_migration_destination(another, requested_node_id=""))
            handler.routing_store.begin_sandbox_migration(
                source, migration_id="capacity-migration", destination_node_id="destination",
                destination_job_id="dest-job", destination_node_url="http://dest:8090",
            )
            self.assertIsNone(handler._select_migration_destination(another, requested_node_id=""))

    def test_publication_is_generation_fenced_and_never_parks_running_work(self):
        with TemporaryDirectory() as directory:
            fixture = direct_fixtures.DirectProvisionerTests()
            provisioner, *_ = fixture.make(Path(directory).resolve())
            service = DirectSandboxService(provisioner)
            fixture.create(service, fixture.spec())
            generation = service.get_snapshot("sandbox").generation
            with patch.object(service, "_start_storage_publication") as publish:
                with self.assertRaisesRegex(RuntimeError, "only parked"):
                    service.request_storage_publication("sandbox", generation=generation)
                service.park("sandbox", operation_id="park:capacity")
                with self.assertRaisesRegex(RuntimeError, "generation"):
                    service.request_storage_publication("sandbox", generation=generation+1)
                publish.assert_not_called()
                service.request_storage_publication("sandbox", generation=generation)
                publish.assert_called_once()

    def test_publication_queue_is_bounded_and_deduplicated(self):
        with TemporaryDirectory() as directory:
            fixture = direct_fixtures.DirectProvisionerTests()
            provisioner, *_ = fixture.make(Path(directory).resolve())
            service = DirectSandboxService(provisioner)
            with patch("ucloud_sandboxes.direct_service.threading.Thread") as thread:
                thread.return_value.is_alive.return_value = True
                for index in range(100):
                    service._start_storage_publication(SimpleNamespace(sandbox_id=str(index), sandbox_generation=1), operation_id="publish:test")
                self.assertEqual(thread.call_count, 16)
                service._start_storage_publication(SimpleNamespace(sandbox_id="0", sandbox_generation=1), operation_id="publish:retry")
                self.assertEqual(thread.call_count, 16)

    def test_cpu_and_storage_pressure_are_refreshed_before_remote_wake(self):
        for pressure in ({'cpu_percent': 95}, {'storage_max_concurrent_operations': 8, 'storage_waiting_operations': 8}):
            with self.subTest(pressure=pressure), TemporaryDirectory() as directory:
                root = Path(directory)
                handler = object.__new__(control_plane.ControlPlaneHandler)
                handler.routing_store = RoutingStore(root / 'routes.sqlite')
                handler.store = ControlStateStore(root / 'control-state.sqlite')
                handler.heartbeat_ttl_seconds = 120
                healthy = self.heartbeat(active=0)
                pressured = replace(healthy, runtime_metrics=replace(healthy.runtime_metrics, **pressure))
                handler.store.upsert_heartbeat(pressured)
                route = handler.routing_store.upsert_sandbox(self.route())
                handler._write_json = Mock()
                handler._select_migration_destination = Mock(return_value=None)
                import json
                handler._proxy_request = Mock(return_value=control_plane.ProxiedResponse(200, {}, json.dumps({
                    'heartbeat': heartbeat_to_dict(healthy),
                }).encode()))
                result = fixtures._prepare_wake_route(handler, route)
                self.assertEqual(result.state, 'waking')
                self.assertEqual(result.job_id, route.job_id)
                self.assertEqual(handler.routing_store.sandbox_migrations(), [])
                handler._write_json.assert_not_called()
                self.assertEqual(handler._proxy_request.call_count, 1)
                self.assertEqual(handler._proxy_request.call_args.args[1], '/v1/heartbeat')
