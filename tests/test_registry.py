from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from threading import Barrier
import unittest

from ucloud_sandboxes.agent import build_heartbeat as _build_heartbeat
from ucloud_sandboxes.bootstrap import VmBootstrapRecord
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.deployment import AGENT_VERSION_LABEL, package_version
from ucloud_sandboxes.models import (
    InstancePhase,
    ResourceQuantity,
    ScalePolicy,
    ProviderInstance,
    utc_now,
)
from ucloud_sandboxes.registry import (
    HeartbeatIdentityError,
    heartbeat_from_dict,
    heartbeat_to_dict,
    merge_jobs_and_heartbeats,
)


def build_heartbeat(**kwargs):
    kwargs.setdefault("deployment_id", "test-deployment")
    return _build_heartbeat(**kwargs)


class RegistryTests(unittest.TestCase):
    def test_control_state_first_open_is_fenced_and_schema_is_exact(self) -> None:
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

            store = ControlStateStore(path)
            with store._transaction(write=True):
                for candidate in (
                    path,
                    path.with_name(f"{path.name}-wal"),
                    path.with_name(f"{path.name}-shm"),
                ):
                    self.assertEqual(candidate.stat().st_mode & 0o777, 0o600)

            with sqlite3.connect(path) as connection:
                self.assertNotEqual(
                    connection.execute("PRAGMA application_id").fetchone(),
                    (0,),
                )
                self.assertEqual(
                    connection.execute("PRAGMA user_version").fetchone(),
                    (1,),
                )
                self.assertEqual(
                    {
                        row[0]
                        for row in connection.execute(
                            "SELECT name FROM sqlite_schema "
                            "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                        )
                    },
                    {"control_records"},
                )
                self.assertEqual(
                    {
                        row[0]
                        for row in connection.execute(
                            "SELECT strict FROM pragma_table_list "
                            "WHERE name = 'control_records'"
                        )
                    },
                    {1},
                )
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
            self.assertIn("job-1", ControlStateStore(path).load_heartbeats())

    def test_rejects_malformed_heartbeat_accounting_fields(self) -> None:
        raw = heartbeat_to_dict(build_heartbeat(job_id="job-1", node_id="node-1"))
        invalid_payloads = (
            {**raw, "active_sandboxes": "not-an-integer"},
            {**raw, "active_image_builds": -1},
            {**raw, "used_resources": {"memory_mb": -1}},
            {**raw, "labels": ["not", "an", "object"]},
        )

        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                self.assertIsNone(heartbeat_from_dict(payload))

    def test_complete_inventory_fails_closed_on_missing_or_invalid_entries(
        self,
    ) -> None:
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

    def test_destructive_heartbeat_flags_and_inventory_resources_are_strict(
        self,
    ) -> None:
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
        job = ProviderInstance(
            id="123",
            name="ucloud-sandbox-node-123",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="IN_QUEUE",
            phase=InstancePhase.PROVISIONING,
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
        job = ProviderInstance(
            id="123",
            name="ucloud-sandbox-node-123",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-16-vcpu",
            product_category="cpu-amd-zen5",
            state="SUSPENDED",
            phase=InstancePhase.PROVISIONING,
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
        job = ProviderInstance(
            id="123",
            name="ucloud-sandbox-node-123",
            application_name="vm-ubuntu",
            application_version="24.04",
            product_id="cpu-amd-zen5-2-vcpu",
            product_category="cpu-amd-zen5",
            state="IN_QUEUE",
            phase=InstancePhase.PROVISIONING,
            cpu=2,
        )

        nodes = merge_jobs_and_heartbeats(
            [job],
            {},
            ScalePolicy(),
        )

        self.assertFalse(nodes[0].agent_version_compatible)
        self.assertTrue(nodes[0].is_provisioning)

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

    def test_retired_boot_epoch_cannot_return_with_a_later_receipt(self) -> None:
        with TemporaryDirectory() as raw_dir:
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
            second = replace(
                first,
                received_at=first.received_at + timedelta(seconds=1),
                node_epoch="boot-b",
                activity_epoch=200,
            )
            delayed_first = replace(
                first,
                received_at=first.received_at + timedelta(seconds=2),
                activity_epoch=150,
            )

            self.assertTrue(store.receive_heartbeat(first).accepted)
            self.assertTrue(store.receive_heartbeat(second).accepted)
            self.assertFalse(store.receive_heartbeat(delayed_first).accepted)
            stored = store.load_heartbeats()["job-1"]

        self.assertEqual(stored.node_epoch, "boot-b")
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

    def test_heartbeat_schema_requires_every_canonical_field(self) -> None:
        raw = heartbeat_to_dict(
            build_heartbeat(
                job_id="job-1",
                node_id="node-1",
                total_resources=ResourceQuantity(),
            )
        )
        missing_deployment = dict(raw)
        missing_deployment.pop("deployment_id")
        empty_deployment = {**raw, "deployment_id": "  "}
        missing_resource_knowledge = dict(raw)
        missing_resource_knowledge.pop("resources_known")
        missing_admission_state = dict(raw)
        missing_admission_state.pop("admission_open")
        null_admission_state = {**raw, "admission_open": None}
        null_inventory = {**raw, "inventory": None}

        self.assertIsNone(heartbeat_from_dict(missing_deployment))
        self.assertIsNone(heartbeat_from_dict(empty_deployment))
        self.assertIsNone(heartbeat_from_dict(missing_resource_knowledge))
        self.assertIsNone(heartbeat_from_dict(missing_admission_state))
        self.assertIsNone(heartbeat_from_dict(null_admission_state))
        self.assertIsNone(heartbeat_from_dict(null_inventory))
        parsed = heartbeat_from_dict(raw)
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertTrue(parsed.resources_known)

    def test_malformed_heartbeat_numbers_are_rejected(self) -> None:
        raw = heartbeat_to_dict(build_heartbeat(job_id="job-1", node_id="node-1"))
        malformed = (
            {**raw, "activity_epoch": "1"},
            {**raw, "physical_disk_total_mb": 1.0},
            {**raw, "physical_disk_free_mb": None},
            {**raw, "active_sandboxes": "1"},
        )

        for payload in malformed:
            with self.subTest(payload=payload):
                self.assertIsNone(heartbeat_from_dict(payload))

    def test_heartbeat_rejects_unknown_fields_and_string_list_aliases(self) -> None:
        raw = heartbeat_to_dict(build_heartbeat(job_id="job-1", node_id="node-1"))
        malformed = (
            {**raw, "future_field": True},
            {**raw, "cpu_overcommit": 1.0},
            {**raw, "capabilities": "sandbox,image-cache"},
            {**raw, "capabilities": [" sandbox"]},
            {**raw, "capabilities": ["sandbox", "sandbox"]},
            {**raw, "cached_images": "image-1"},
            {**raw, "retired_node_epochs": [1]},
        )

        for payload in malformed:
            with self.subTest(payload=payload):
                self.assertIsNone(heartbeat_from_dict(payload))

    def test_heartbeat_rejects_noncanonical_resource_fields(self) -> None:
        timestamp = utc_now().isoformat()
        payloads = (
            {
                "node_id": "node-1",
                "job_id": "job-1",
                "updated_at": timestamp,
                "totalResources": {"vcpu": 4},
            },
            {
                "node_id": "node-1",
                "job_id": "job-1",
                "updated_at": timestamp,
                "total_resources": {"cpu": 4},
            },
            {
                "node_id": "node-1",
                "job_id": "job-1",
                "updated_at": timestamp,
                "total_resources": {"memoryMb": 4096},
            },
        )
        for payload in payloads:
            with self.subTest(payload=payload):
                self.assertIsNone(heartbeat_from_dict(payload))

    def test_heartbeat_store_removes_jobs(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "control-state.sqlite"
            store = ControlStateStore(path)
            for job_id, node_id in (("job-1", "node-1"), ("job-2", "node-2")):
                store.upsert_heartbeat(build_heartbeat(job_id=job_id, node_id=node_id))

            removed = store.remove_heartbeats(("job-1", "missing"))
            loaded = store.load_heartbeats()

            self.assertEqual(tuple(removed), ("job-1",))
            self.assertNotIn("job-1", loaded)
            self.assertIn("job-2", loaded)

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

    def test_heartbeat_store_tracks_idle_since_transition(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "control-state.sqlite"
            store = ControlStateStore(path)
            busy_at = utc_now()
            idle_at = busy_at + timedelta(seconds=30)
            later_at = idle_at + timedelta(seconds=30)

            store.upsert_heartbeat(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=1,
                    now=busy_at,
                )
            )
            self.assertIsNone(store.load_heartbeats()["job-1"].idle_since)

            store.upsert_heartbeat(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=0,
                    now=idle_at,
                )
            )
            self.assertEqual(store.load_heartbeats()["job-1"].idle_since, idle_at)

            store.upsert_heartbeat(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_sandboxes=0,
                    now=later_at,
                )
            )
            self.assertEqual(store.load_heartbeats()["job-1"].idle_since, idle_at)

    def test_heartbeat_store_treats_image_build_as_active_work(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "control-state.sqlite"
            store = ControlStateStore(path)
            busy_at = utc_now()
            idle_at = busy_at + timedelta(seconds=30)

            store.upsert_heartbeat(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_image_builds=1,
                    now=busy_at,
                )
            )
            self.assertIsNone(store.load_heartbeats()["job-1"].idle_since)

            store.upsert_heartbeat(
                build_heartbeat(
                    job_id="job-1",
                    node_id="node-1",
                    active_image_builds=0,
                    now=idle_at,
                )
            )
            self.assertEqual(store.load_heartbeats()["job-1"].idle_since, idle_at)


if __name__ == "__main__":
    unittest.main()
