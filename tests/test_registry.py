from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.agent import build_heartbeat
from ucloud_sandboxes.deployment import AGENT_VERSION_LABEL, package_version
from ucloud_sandboxes.models import (
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxInventoryEntry,
    ScalePolicy,
    VmJob,
    utc_now,
)
from ucloud_sandboxes.registry import (
    HeartbeatStore,
    heartbeat_from_dict,
    heartbeat_to_dict,
    merge_jobs_and_heartbeats,
)


class RegistryTests(unittest.TestCase):
    def test_heartbeat_state_is_durable_and_owner_only(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "heartbeats.json"
            HeartbeatStore(path).upsert(
                build_heartbeat(job_id="job-1", node_id="node-1")
            )

            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertIn("job-1", HeartbeatStore(path).load())

    def test_rejects_malformed_legacy_heartbeat_accounting_fields(self) -> None:
        raw = heartbeat_to_dict(build_heartbeat(job_id="job-1", node_id="node-1"))
        invalid_payloads = (
            {**raw, "active_sandboxes": "not-an-integer"},
            {**raw, "active_image_builds": -1},
            {**raw, "cpu_overcommit": "nan"},
            {**raw, "used_resources": {"memory_mb": -1}},
            {**raw, "labels": ["not", "an", "object"]},
        )

        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                self.assertIsNone(heartbeat_from_dict(payload))

    def test_complete_inventory_fails_closed_on_missing_or_invalid_entries(self) -> None:
        raw = heartbeat_to_dict(build_heartbeat(job_id="job-1", node_id="node-1"))
        raw["inventory_complete"] = True

        missing_inventory = dict(raw)
        missing_inventory.pop("inventory", None)
        invalid_inventory = {
            **raw,
            "inventory": [{"sandbox_id": "sandbox-1", "generation": 1}],
        }
        mixed_inventory = {
            **raw,
            "inventory": [
                {
                    "sandbox_id": "sandbox-1",
                    "generation": 1,
                    "operation_id": "operation-1",
                    "spec_hash": "sha256:spec",
                    "state": "running",
                },
                {"sandbox_id": "sandbox-2", "generation": 2},
            ],
        }

        self.assertIsNone(heartbeat_from_dict(missing_inventory))
        self.assertIsNone(heartbeat_from_dict(invalid_inventory))
        self.assertIsNone(heartbeat_from_dict(mixed_inventory))

    def test_destructive_heartbeat_flags_and_inventory_resources_are_strict(self) -> None:
        raw = heartbeat_to_dict(build_heartbeat(job_id="job-1", node_id="node-1"))
        malformed = (
            {**raw, "inventory_complete": "true"},
            {**raw, "admission_open": "false"},
            {**raw, "draining": 1},
            {**raw, "cached_images_known": "false"},
            {
                **raw,
                "inventory_complete": True,
                "inventory": [
                    {
                        "sandbox_id": "sandbox-1",
                        "generation": 1,
                        "operation_id": "operation-1",
                        "spec_hash": "sha256:spec",
                        "resources": {"memory_mb": -1},
                    }
                ],
            },
        )

        for payload in malformed:
            with self.subTest(payload=payload):
                self.assertIsNone(heartbeat_from_dict(payload))

    def test_marks_provisioning_node_without_heartbeat(self) -> None:
        job = VmJob(
            id="123",
            project_id="project-1",
            name="ucloud-sandbox-node-123",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="IN_QUEUE",
            cpu=2,
            labels={AGENT_VERSION_LABEL: package_version()},
        )

        nodes = merge_jobs_and_heartbeats(
            [job],
            {},
            ScalePolicy(),
        )

        self.assertTrue(nodes[0].is_provisioning)

    def test_marks_suspended_vm_as_provisioning(self) -> None:
        job = VmJob(
            id="123",
            project_id="project-1",
            name="ucloud-sandbox-node-123",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-16-vcpu",
            product_category="cpu-amd-zen5",
            state="SUSPENDED",
            cpu=16,
            disk_gb=250,
            labels={AGENT_VERSION_LABEL: package_version()},
        )

        nodes = merge_jobs_and_heartbeats(
            [job],
            {},
            ScalePolicy(),
        )

        self.assertTrue(nodes[0].is_provisioning)

    def test_unversioned_provisioning_node_is_incompatible(self) -> None:
        job = VmJob(
            id="123",
            project_id="project-1",
            name="ucloud-sandbox-node-123",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="IN_QUEUE",
            cpu=2,
        )

        nodes = merge_jobs_and_heartbeats(
            [job],
            {},
            ScalePolicy(),
        )

        self.assertFalse(nodes[0].agent_version_compatible)
        self.assertTrue(nodes[0].is_provisioning)

    def test_heartbeat_store_roundtrip(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "heartbeats.json"
            heartbeat = build_heartbeat(
                job_id="job-1",
                node_id="node-1",
                node_url="http://node-1:8090",
                active_sandboxes=2,
                active_image_builds=1,
                agent_version="0.1.0-test",
                deployment_id="prod-a",
                init_version="init-1",
                capabilities=("sandbox", "image-build"),
                labels={"role": "worker"},
                runtime_metrics=NodeRuntimeMetrics(
                    collected_at=utc_now(),
                    cpu_percent=10.0,
                    cpu_vcpu=0.2,
                    cpu_count=2,
                    memory_total_mb=6144,
                    memory_used_mb=1024,
                    memory_available_mb=5120,
                    memory_percent=16.6666666667,
                ),
            )

            store = HeartbeatStore(path)
            store.upsert(heartbeat)
            loaded = store.load()

            self.assertIn("job-1", loaded)
            self.assertEqual(loaded["job-1"].node_id, "node-1")
            self.assertEqual(loaded["job-1"].node_url, "http://node-1:8090")
            self.assertEqual(loaded["job-1"].agent_version, "0.1.0-test")
            self.assertEqual(loaded["job-1"].deployment_id, "prod-a")
            self.assertEqual(loaded["job-1"].init_version, "init-1")
            self.assertEqual(loaded["job-1"].active_sandboxes, 2)
            self.assertEqual(loaded["job-1"].active_image_builds, 1)
            self.assertEqual(loaded["job-1"].capabilities, ("sandbox", "image-build"))
            self.assertEqual(loaded["job-1"].labels, {"role": "worker"})
            self.assertIsNotNone(loaded["job-1"].runtime_metrics)
            assert loaded["job-1"].runtime_metrics is not None
            self.assertEqual(loaded["job-1"].runtime_metrics.cpu_percent, 10.0)
            self.assertEqual(loaded["job-1"].runtime_metrics.memory_used_mb, 1024)

    def test_heartbeat_store_roundtrips_distributed_state_fields(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "heartbeats.json"
            reported_at = utc_now()
            received_at = reported_at + timedelta(seconds=3)
            heartbeat = replace(
                build_heartbeat(job_id="job-1", node_id="node-1", now=reported_at),
                reported_at=reported_at,
                received_at=received_at,
                node_epoch="boot-123",
                activity_epoch=7,
                inventory=(
                    SandboxInventoryEntry(
                        sandbox_id="sandbox-1",
                        generation=4,
                        operation_id="operation-1",
                        spec_hash="sha256:spec",
                        state="running",
                        resources=ResourceQuantity(vcpu=2, memory_mb=1024, disk_mb=2048),
                    ),
                ),
                inventory_complete=True,
                reserved_resources=ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=64),
                build_reserved_resources=ResourceQuantity(
                    vcpu=2,
                    memory_mb=2048,
                    disk_mb=4096,
                ),
                physical_disk_total_mb=100_000,
                physical_disk_free_mb=40_000,
                drain_token="drain-1",
                drain_activity_epoch=7,
                admission_open=False,
            )

            HeartbeatStore(path).upsert(heartbeat)
            loaded = HeartbeatStore(path).load()["job-1"]

            self.assertEqual(loaded.reported_at, reported_at)
            self.assertEqual(loaded.received_at, received_at)
            self.assertEqual(loaded.node_epoch, "boot-123")
            self.assertEqual(loaded.activity_epoch, 7)
            self.assertTrue(loaded.inventory_complete)
            self.assertEqual(loaded.inventory, heartbeat.inventory)
            self.assertEqual(loaded.reserved_resources, heartbeat.reserved_resources)
            self.assertEqual(
                loaded.build_reserved_resources,
                heartbeat.build_reserved_resources,
            )
            self.assertEqual(loaded.physical_disk_total_mb, 100_000)
            self.assertEqual(loaded.physical_disk_free_mb, 40_000)
            self.assertEqual(loaded.drain_token, "drain-1")
            self.assertEqual(loaded.drain_activity_epoch, 7)
            self.assertFalse(loaded.admission_open)

    def test_idle_transition_uses_gateway_receipt_time(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "heartbeats.json"
            store = HeartbeatStore(path)
            node_reported_at = utc_now() - timedelta(hours=1)
            received_at = utc_now()

            store.upsert(
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

            self.assertEqual(store.load()["job-1"].idle_since, received_at)

    def test_malformed_additive_heartbeat_numbers_fall_back_safely(self) -> None:
        heartbeat = heartbeat_from_dict(
            {
                "node_id": "node-1",
                "job_id": "job-1",
                "updated_at": utc_now().isoformat(),
                "activity_epoch": "not-an-integer",
                "physical_disk_total_mb": "unknown",
                "physical_disk_free_mb": [],
            }
        )

        self.assertIsNotNone(heartbeat)
        assert heartbeat is not None
        self.assertEqual(heartbeat.activity_epoch, 0)
        self.assertEqual(heartbeat.physical_disk_total_mb, 0)
        self.assertEqual(heartbeat.physical_disk_free_mb, 0)

    def test_heartbeat_store_removes_jobs(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "heartbeats.json"
            store = HeartbeatStore(path)
            store.save(
                {
                    "job-1": build_heartbeat(job_id="job-1", node_id="node-1"),
                    "job-2": build_heartbeat(job_id="job-2", node_id="node-2"),
                }
            )

            removed = store.remove(("job-1", "missing"))
            loaded = store.load()

            self.assertEqual(tuple(removed), ("job-1",))
            self.assertNotIn("job-1", loaded)
            self.assertIn("job-2", loaded)

    def test_heartbeat_store_quarantines_corrupt_json(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "heartbeats.json"
            path.write_text('{"nodes": []}\n{"nodes": []}\n', encoding="utf-8")
            store = HeartbeatStore(path)

            loaded = store.load()

            self.assertEqual(loaded, {})
            self.assertFalse(path.exists())
            self.assertEqual(
                len(list(Path(raw_dir).glob("heartbeats.json.corrupt-*"))),
                1,
            )

    def test_heartbeat_store_tracks_idle_since_transition(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "heartbeats.json"
            store = HeartbeatStore(path)
            busy_at = utc_now()
            idle_at = busy_at + timedelta(seconds=30)
            later_at = idle_at + timedelta(seconds=30)

            store.upsert(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=1,
                    now=busy_at,
                )
            )
            self.assertIsNone(store.load()["job-1"].idle_since)

            store.upsert(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=0,
                    now=idle_at,
                )
            )
            self.assertEqual(store.load()["job-1"].idle_since, idle_at)

            store.upsert(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=0,
                    now=later_at,
                )
            )
            self.assertEqual(store.load()["job-1"].idle_since, idle_at)

    def test_heartbeat_store_treats_image_build_as_active_work(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "heartbeats.json"
            store = HeartbeatStore(path)
            busy_at = utc_now()
            idle_at = busy_at + timedelta(seconds=30)

            store.upsert(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_image_builds=1,
                    now=busy_at,
                )
            )
            self.assertIsNone(store.load()["job-1"].idle_since)

            store.upsert(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_image_builds=0,
                    now=idle_at,
                )
            )
            self.assertEqual(store.load()["job-1"].idle_since, idle_at)


if __name__ == "__main__":
    unittest.main()
