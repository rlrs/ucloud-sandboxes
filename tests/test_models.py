import unittest
from datetime import timedelta

from ucloud_sandboxes.models import (
    InstancePhase,
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxInventoryEntry,
    parse_iso_datetime,
    utc_now,
)
from ucloud_sandboxes.providers.ucloud.models import instance_from_payload


class TimestampParsingTests(unittest.TestCase):
    def test_accepts_go_rfc3339_nanoseconds_on_python_310(self) -> None:
        parsed = parse_iso_datetime("2026-08-27T13:05:18.123456789Z")

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.isoformat(), "2026-08-27T13:05:18.123456+00:00")

    def test_rejects_invalid_high_precision_timestamp(self) -> None:
        self.assertIsNone(parse_iso_datetime("2026-08-27T13:05:18.123456789oops"))


class VmJobParsingTests(unittest.TestCase):
    def test_parses_ucloud_vm_job_shape(self) -> None:
        payload = {
            "id": "12345311",
            "createdAt": 1782638330055,
            "owner": {"project": "project-1"},
            "updates": [{"status": "queue full"}],
            "specification": {
                "product": {
                    "id": "cpu-amd-zen5-2-vcpu",
                    "category": "cpu-amd-zen5",
                    "provider": "ucloud",
                },
                "application": {"name": "vm-ubuntu", "version": "24.04"},
                "hostname": "ubuntu-8263",
                "labels": {"ucloud-sandboxes/deployment": "prod-a"},
                "resources": [{"type": "private_network", "id": "net-1"}],
                "parameters": {
                    "diskSize": {"value": 50},
                },
            },
            "status": {
                "state": "IN_QUEUE",
                "jobParametersJson": {
                    "request": {
                        "sshEnabled": False,
                        "resolvedProduct": {
                            "cpu": 2,
                            "memoryInGigs": 6,
                        },
                        "resolvedSupport": {
                            "support": {
                                "queueStatus": "FULL",
                            },
                        },
                    },
                    "machineType": {"cpu": 2, "memoryInGigs": 6},
                },
            },
        }

        job = instance_from_payload(payload)

        self.assertEqual(job.id, "12345311")
        self.assertEqual(job.state, "IN_QUEUE")
        self.assertEqual(job.product_id, "cpu-amd-zen5-2-vcpu")
        self.assertEqual(job.cpu, 2)
        self.assertEqual(job.memory_gb, 6)
        self.assertEqual(job.disk_gb, 50)
        self.assertEqual(job.labels, {"ucloud-sandboxes/deployment": "prod-a"})
        self.assertEqual(job.private_network_ids, ("net-1",))
        self.assertEqual(job.queue_status, "FULL")
        self.assertFalse(job.ssh_enabled)

    def test_only_post_start_suspension_is_destructive(self) -> None:
        def job_with_updates(updates, *, state="RUNNING"):
            return instance_from_payload(
                {
                    "id": "vm-1",
                    "createdAt": 1_700_000_000_000,
                    "owner": {"project": "project-1"},
                    "specification": {
                        "application": {"name": "vm-ubuntu", "version": "24.04"},
                        "product": {"id": "cpu-amd-zen5-2-vcpu"},
                    },
                    "status": {
                        "state": state,
                        "startedAt": 1_700_000_100_000,
                    },
                    "updates": updates,
                }
            )

        initial_boot = job_with_updates([{"state": "SUSPENDED"}, {"state": "RUNNING"}])
        power_cycled = job_with_updates(
            [
                {"state": "SUSPENDED"},
                {"state": "RUNNING"},
                {"state": "SUSPENDED"},
                {"state": "RUNNING"},
            ]
        )
        currently_suspended = job_with_updates([], state="SUSPENDED")

        self.assertEqual(initial_boot.phase, InstancePhase.RUNNING)
        self.assertEqual(power_cycled.phase, InstancePhase.LOST)
        self.assertEqual(currently_suspended.phase, InstancePhase.LOST)


class HeartbeatContractTests(unittest.TestCase):
    def test_runtime_metrics_reject_malformed_or_noncanonical_values(self) -> None:
        metrics = NodeRuntimeMetrics(collected_at=utc_now(), cpu_percent=12.5)
        canonical = metrics.to_dict()
        malformed = (
            {**canonical, "cpu_percent": "12.5"},
            {**canonical, "cpu_percent": True},
            {**canonical, "cpu_vcpu": float("nan")},
            {**canonical, "cpu_count": "1"},
            {**canonical, "cpu_count": True},
            {**canonical, "memory_total_mb": -1},
            {**canonical, "storage_device_pool_enabled": 1},
            {key: value for key, value in canonical.items() if key != "cpu_count"},
            {**canonical, "imagePullActiveOperations": 3},
        )

        for payload in malformed:
            with self.subTest(payload=payload):
                self.assertIsNone(NodeRuntimeMetrics.from_dict(payload))

        self.assertEqual(canonical["collected_at"], metrics.collected_at.isoformat())
        self.assertEqual(NodeRuntimeMetrics.from_dict(canonical), metrics)

    def test_resource_quantity_rejects_permissive_values(self) -> None:
        with self.assertRaises(ValueError):
            ResourceQuantity.from_dict(
                {"vcpu": "nan", "memory_mb": -1, "disk_mb": "invalid"}
            )

    def test_gateway_receipt_time_controls_freshness(self) -> None:
        now = utc_now()
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            updated_at=now + timedelta(days=1),
            received_at=now - timedelta(seconds=11),
            active_sandboxes=0,
        )

        self.assertFalse(heartbeat.is_fresh(now, ttl_seconds=10))

    def test_untrusted_future_timestamp_does_not_stay_fresh_forever(self) -> None:
        now = utc_now()
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            updated_at=now + timedelta(seconds=1),
            active_sandboxes=0,
        )

        self.assertFalse(heartbeat.is_fresh(now, ttl_seconds=10))

    def test_inventory_requires_complete_incarnation_identity(self) -> None:
        base = {
            "sandbox_id": "sandbox-1",
            "generation": 3,
            "operation_id": "operation-7",
            "spec_hash": "sha256:abc",
        }

        for missing in ("operation_id", "spec_hash"):
            malformed = dict(base)
            malformed[missing] = ""
            self.assertIsNone(SandboxInventoryEntry.from_dict(malformed))

        self.assertIsNone(
            SandboxInventoryEntry.from_dict(
                {"sandbox_id": "sandbox-1", "generation": 0}
            )
        )

    def test_estimates_cpu_from_vm_product_id_when_resolved_product_is_absent(
        self,
    ) -> None:
        payload = {
            "id": "123",
            "specification": {
                "product": {
                    "id": "cpu-amd-zen5-2-vcpu",
                    "category": "cpu-amd-zen5",
                },
                "application": {"name": "vm-ubuntu", "version": "24.04"},
            },
            "status": {"state": "IN_QUEUE"},
        }

        job = instance_from_payload(payload)

        self.assertEqual(job.cpu, 2)


if __name__ == "__main__":
    unittest.main()
