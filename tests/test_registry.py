from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import timedelta
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Barrier
import unittest

from ucloud_sandboxes.agent import build_heartbeat as _build_heartbeat
from ucloud_sandboxes.bootstrap import VmBootstrapRecord
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.models import (
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxInventoryEntry,
    utc_now,
)
from ucloud_sandboxes.registry import (
    HeartbeatIdentityError,
    heartbeat_from_dict,
    heartbeat_to_dict,
)


def build_heartbeat(**kwargs):
    kwargs.setdefault("deployment_id", "test-deployment")
    return _build_heartbeat(**kwargs)


class RegistryTests(unittest.TestCase):
    def test_heartbeat_serializer_matches_deep_dataclass_projection(self) -> None:
        now = utc_now()
        cached_images = tuple(
            f"registry.example/image@sha256:{index:064x}" for index in range(600)
        )
        heartbeat = replace(
            build_heartbeat(
                job_id="job-rich",
                node_id="node-rich",
                active_sandboxes=1,
                active_image_builds=2,
                active_sandbox_creates=3,
                draining=True,
                node_url="http://node-rich:8090",
                agent_version="0.5.12",
                deployment_id="production",
                init_version="init-v2",
                capabilities=("sandbox", "image-cache", "storage-native-v1"),
                total_resources=ResourceQuantity(
                    vcpu=32.0,
                    memory_mb=131_072,
                    disk_mb=2_000_000,
                ),
                used_resources=ResourceQuantity(
                    vcpu=3.5,
                    memory_mb=12_345,
                    disk_mb=67_890,
                ),
                labels={"provider": "ucloud", "pool": "cpu"},
                cached_images=cached_images,
                runtime_metrics=NodeRuntimeMetrics(
                    collected_at=now - timedelta(seconds=1),
                    cpu_percent=37.5,
                    cpu_vcpu=12.0,
                    cpu_count=32,
                    memory_total_mb=131_072,
                    memory_used_mb=12_345,
                    memory_available_mb=118_727,
                    memory_percent=9.4,
                    memory_psi_some_avg10=0.25,
                    load_average_1m=4.5,
                    storage_hard_capacity_mb=2_000_000,
                    storage_hard_reserved_mb=67_890,
                    storage_device_pool_enabled=True,
                    storage_device_pool_idle_devices=4,
                    storage_ublk_active_devices=1,
                    storage_ublk_live_devices=2,
                    storage_ublk_max_devices=64,
                ),
                node_epoch="boot-current",
                activity_epoch=41,
                inventory=(
                    SandboxInventoryEntry(
                        sandbox_id="sandbox-1",
                        generation=7,
                        operation_id="operation-7",
                        spec_hash="a" * 64,
                        state="running",
                        resources=ResourceQuantity(
                            vcpu=1.5,
                            memory_mb=2_048,
                            disk_mb=4_096,
                        ),
                        storage_schema="oci-native-v1",
                        snapshot_manifest_digest=f"sha256:{'b' * 64}",
                        snapshot_repository="ucloud/snapshots",
                        snapshot_tag="sandbox-1-generation-7",
                        storage_snapshot={
                            "driver": "ublk",
                            "layers": [f"sha256:{'c' * 64}"],
                        },
                    ),
                ),
                inventory_complete=True,
                reserved_resources=ResourceQuantity(
                    vcpu=2.0,
                    memory_mb=4_096,
                    disk_mb=8_192,
                ),
                build_reserved_resources=ResourceQuantity(
                    vcpu=1.0,
                    memory_mb=1_024,
                    disk_mb=16_384,
                ),
                physical_disk_total_mb=2_100_000,
                physical_disk_free_mb=1_900_000,
                drain_token="drain-1",
                drain_activity_epoch=40,
                admission_open=False,
                now=now,
            ),
            idle_since=now - timedelta(minutes=2),
            received_at=now + timedelta(milliseconds=10),
            retired_node_epochs=("boot-old-1", "boot-old-2"),
        )

        payload = heartbeat_to_dict(heartbeat)
        legacy_payload = json.loads(
            json.dumps(asdict(heartbeat), default=lambda value: value.isoformat())
        )

        self.assertEqual(payload, legacy_payload)
        self.assertEqual(heartbeat_from_dict(payload), heartbeat)
        self.assertEqual(len(payload["cached_images"]), 600)

        payload["labels"]["provider"] = "changed"
        payload["inventory"][0]["storage_snapshot"]["driver"] = "changed"
        self.assertEqual(heartbeat.labels["provider"], "ucloud")
        self.assertEqual(heartbeat.inventory[0].storage_snapshot["driver"], "ublk")

    def test_control_state_first_open_is_fenced_and_rejects_schema_drift(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "control-state.sqlite"
            barrier = Barrier(5)

            def open_store() -> None:
                barrier.wait()
                ControlStateStore(path)

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(open_store) for _ in range(4)]
                barrier.wait()
                for future in futures:
                    future.result()

            with sqlite3.connect(path) as connection:
                connection.execute(
                    "CREATE INDEX unexpected_control_state_index "
                    "ON control_records(record_id)"
                )

            with self.assertRaisesRegex(ValueError, "unsupported control state"):
                ControlStateStore(path)

            with sqlite3.connect(path) as connection:
                connection.execute("DROP INDEX unexpected_control_state_index")
                connection.execute("PRAGMA user_version = 2")
            with self.assertRaisesRegex(ValueError, "unsupported control state"):
                ControlStateStore(path)

    def test_heartbeat_state_is_durable_and_owner_only(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "control-state.sqlite"
            ControlStateStore(path).upsert_heartbeat(
                build_heartbeat(job_id="job-1", node_id="node-1")
            )

            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            reopened = ControlStateStore(path)
            self.assertIn("job-1", reopened.load_heartbeats())
            self.assertEqual(reopened.get_heartbeat("job-1").node_id, "node-1")
            self.assertIsNone(reopened.get_heartbeat("missing"))

    def test_control_state_accepts_canonical_pre_device_limit_metrics(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "control-state.sqlite"
            heartbeat = replace(
                build_heartbeat(job_id="job-1", node_id="node-1"),
                runtime_metrics=NodeRuntimeMetrics(collected_at=utc_now()),
            )
            store = ControlStateStore(path)
            store.upsert_heartbeat(heartbeat)
            with sqlite3.connect(path) as connection:
                payload = json.loads(
                    connection.execute(
                        "SELECT payload FROM control_records "
                        "WHERE namespace = 'heartbeat' AND record_id = 'job-1'"
                    ).fetchone()[0]
                )
                payload["runtime_metrics"].pop("storage_ublk_max_devices")
                connection.execute(
                    "UPDATE control_records SET payload = ? "
                    "WHERE namespace = 'heartbeat' AND record_id = 'job-1'",
                    (json.dumps(payload, sort_keys=True, separators=(",", ":")),),
                )

            stored = store.load_heartbeats()["job-1"]

        self.assertEqual(stored.runtime_metrics.storage_ublk_max_devices, 0)

    def test_heartbeat_schema_is_strict_and_fail_closed(self) -> None:
        raw = heartbeat_to_dict(build_heartbeat(job_id="job-1", node_id="node-1"))
        valid_inventory = {
            "sandbox_id": "sandbox-1",
            "generation": 1,
            "operation_id": "operation-1",
            "spec_hash": "sha256:spec",
            "state": "running",
        }

        def missing(field: str, **updates: object) -> dict:
            payload = {**raw, **updates}
            payload.pop(field)
            return payload

        malformed = (
            missing("deployment_id"),
            missing("resources_known"),
            missing("admission_open"),
            missing("inventory", inventory_complete=True),
            {**raw, "deployment_id": "  "},
            {**raw, "admission_open": None},
            {**raw, "inventory": None},
            {**raw, "active_sandboxes": "not-an-integer"},
            {**raw, "active_image_builds": -1},
            {**raw, "activity_epoch": "1"},
            {**raw, "physical_disk_total_mb": 1.0},
            {**raw, "physical_disk_free_mb": None},
            {**raw, "used_resources": {"memory_mb": -1}},
            {**raw, "labels": ["not", "an", "object"]},
            {**raw, "inventory_complete": "true"},
            {**raw, "admission_open": "false"},
            {**raw, "draining": 1},
            {**raw, "cached_images_known": "false"},
            {**raw, "future_field": True},
            {**raw, "cpu_overcommit": 1.0},
            {**raw, "capabilities": "sandbox,image-cache"},
            {**raw, "capabilities": [" sandbox"]},
            {**raw, "capabilities": ["sandbox", "sandbox"]},
            {**raw, "cached_images": "image-1"},
            {**raw, "retired_node_epochs": [1]},
            {**raw, "totalResources": {"vcpu": 4}},
            {**raw, "total_resources": {"cpu": 4}},
            {**raw, "total_resources": {"memoryMb": 4096}},
            {
                **raw,
                "inventory_complete": True,
                "inventory": [{"sandbox_id": "sandbox-1", "generation": 1}],
            },
            {
                **raw,
                "inventory_complete": True,
                "inventory": [valid_inventory, {"sandbox_id": "sandbox-2"}],
            },
            {
                **raw,
                "inventory_complete": True,
                "inventory": [{**valid_inventory, "resources": {"memory_mb": -1}}],
            },
        )
        self.assertIsNotNone(heartbeat_from_dict(raw))
        for payload in malformed:
            with self.subTest(payload=payload):
                self.assertIsNone(heartbeat_from_dict(payload))

    def test_idle_transition_uses_gateway_receipt_time(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "control-state.sqlite"
            store = ControlStateStore(path)
            node_reported_at = utc_now() - timedelta(hours=1)
            received_at = utc_now()

            store.upsert_heartbeat(
                replace(
                    build_heartbeat(
                        job_id="job-1",
                        node_id="node-1",
                        active_sandboxes=0,
                        now=node_reported_at,
                    ),
                    reported_at=node_reported_at,
                    received_at=received_at,
                )
            )

            self.assertEqual(store.load_heartbeats()["job-1"].idle_since, received_at)

    def test_older_gateway_receipt_cannot_overwrite_newer_node_state(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "control-state.sqlite"
            store = ControlStateStore(path)
            older_at = utc_now()
            newer_at = older_at + timedelta(seconds=1)
            older = replace(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=0,
                    now=older_at,
                ),
                received_at=older_at,
                node_epoch="old-boot",
                admission_open=True,
                idle_since=None,
            )
            newer = replace(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=1,
                    now=newer_at,
                ),
                received_at=newer_at,
                node_epoch="new-boot",
                admission_open=False,
                idle_since=None,
            )

            newer_result = store.receive_heartbeat(newer)
            older_result = store.receive_heartbeat(older)
            stored = store.load_heartbeats()["job-1"]

            self.assertTrue(newer_result.accepted)
            self.assertFalse(older_result.accepted)
            self.assertEqual(stored.received_at, newer_at)
            self.assertEqual(stored.node_epoch, "new-boot")
            self.assertFalse(stored.admission_open)
            self.assertEqual(stored.active_sandboxes, 1)
            self.assertIsNone(stored.idle_since)

    def test_new_boot_accepts_reset_activity_and_retires_previous_epoch(self) -> None:
        for label, restarted_activity in (("same", 100), ("lower", 1)):
            with self.subTest(activity=label), TemporaryDirectory() as raw_dir:
                store = ControlStateStore(Path(raw_dir) / "control-state.sqlite")
                first = replace(
                    build_heartbeat(
                        job_id="job-1",
                        node_id="node-1",
                        node_url="http://node-1:8090",
                        deployment_id="prod",
                    ),
                    received_at=utc_now(),
                    node_epoch="boot-a",
                    activity_epoch=100,
                )
                assert first.received_at is not None
                restarted = replace(
                    first,
                    received_at=first.received_at + timedelta(seconds=1),
                    node_epoch="boot-b",
                    activity_epoch=restarted_activity,
                )
                delayed_first = replace(
                    first,
                    received_at=first.received_at + timedelta(seconds=2),
                    activity_epoch=150,
                )

                self.assertTrue(store.receive_heartbeat(first).accepted)
                self.assertTrue(store.receive_heartbeat(restarted).accepted)
                self.assertFalse(store.receive_heartbeat(delayed_first).accepted)
                stored = store.load_heartbeats()["job-1"]

                self.assertEqual(stored.node_epoch, "boot-b")
                self.assertEqual(stored.activity_epoch, restarted_activity)
                self.assertEqual(stored.retired_node_epochs, ("boot-a",))

    def test_heartbeat_identity_is_immutable_for_a_job(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = ControlStateStore(Path(raw_dir) / "control-state.sqlite")
            original = replace(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    node_url="http://node-1:8090",
                    deployment_id="prod",
                ),
                received_at=utc_now(),
                node_epoch="boot-a",
                activity_epoch=100,
            )
            assert original.received_at is not None
            spoofed = replace(
                original,
                node_url="http://attacker.invalid:8090",
                received_at=original.received_at + timedelta(seconds=1),
                activity_epoch=101,
            )

            self.assertTrue(store.receive_heartbeat(original).accepted)
            with self.assertRaises(HeartbeatIdentityError):
                store.receive_heartbeat(spoofed)
            stored = store.load_heartbeats()["job-1"]

        self.assertEqual(stored.node_url, original.node_url)

    def test_cross_job_node_binding_is_atomic(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = ControlStateStore(Path(raw_dir) / "control-state.sqlite")
            barrier = Barrier(3)
            received_at = utc_now()

            def submit(job_id: str) -> str:
                heartbeat = replace(
                    build_heartbeat(
                        job_id=job_id,
                        node_id="shared-node",
                        node_url="http://shared-node:8090",
                        node_epoch=f"boot-{job_id}",
                    ),
                    received_at=received_at,
                )
                barrier.wait()
                try:
                    result = store.receive_heartbeat(heartbeat)
                except HeartbeatIdentityError:
                    return "conflict"
                return "accepted" if result.accepted else "stale"

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(submit, job_id) for job_id in ("a", "b")]
                barrier.wait()
                outcomes = sorted(future.result() for future in futures)

            stored = store.load_heartbeats()

        self.assertEqual(outcomes, ["accepted", "conflict"])
        self.assertEqual(len(stored), 1)
        self.assertEqual(next(iter(stored.values())).node_id, "shared-node")

    def test_canonical_node_url_binding_is_atomic(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = ControlStateStore(Path(raw_dir) / "control-state.sqlite")
            barrier = Barrier(3)
            received_at = utc_now()

            def submit(job_id: str, node_url: str) -> str:
                heartbeat = replace(
                    build_heartbeat(
                        job_id=job_id,
                        node_id=f"node-{job_id}",
                        node_url=node_url,
                        node_epoch=f"boot-{job_id}",
                    ),
                    received_at=received_at,
                )
                barrier.wait()
                try:
                    result = store.receive_heartbeat(heartbeat)
                except HeartbeatIdentityError:
                    return "conflict"
                return "accepted" if result.accepted else "stale"

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(submit, "a", "HTTP://SHARED-NODE:8090/"),
                    executor.submit(submit, "b", "http://shared-node:8090"),
                ]
                barrier.wait()
                outcomes = sorted(future.result() for future in futures)

            stored = store.load_heartbeats()

        self.assertEqual(outcomes, ["accepted", "conflict"])
        self.assertEqual(len(stored), 1)

    def test_control_state_fails_closed_on_legacy_json_and_corrupt_rows(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            legacy = root / "heartbeats.json"
            legacy.write_text('{"nodes": []}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unreadable"):
                ControlStateStore(legacy)

            path = root / "control-state.sqlite"
            store = ControlStateStore(path)
            store.upsert_heartbeat(build_heartbeat(job_id="job-1", node_id="node-1"))
            with sqlite3.connect(path) as connection:
                connection.execute(
                    "UPDATE control_records SET payload = ' ' || payload "
                    "WHERE namespace = 'heartbeat'"
                )

            with self.assertRaisesRegex(ValueError, "invalid heartbeat control-state"):
                store.load_heartbeats()

    def test_control_state_namespaces_do_not_overwrite_each_other(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = ControlStateStore(Path(raw_dir) / "control-state.sqlite")
            heartbeat = build_heartbeat(job_id="job-1", node_id="node-1")
            bootstrap = VmBootstrapRecord(job_id="job-1", attempts=1)

            store.save_bootstrap_records({"job-1": bootstrap})
            store.upsert_heartbeat(heartbeat)
            self.assertEqual(
                store.load_bootstrap_records(),
                {"job-1": bootstrap},
            )

            store.save_bootstrap_records({})
            self.assertEqual(
                store.load_heartbeats()["job-1"].node_id,
                heartbeat.node_id,
            )


if __name__ == "__main__":
    unittest.main()
