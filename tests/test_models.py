import unittest
from datetime import timedelta

from ucloud_sandboxes.models import (
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    SandboxInventoryEntry,
    utc_now,
    vm_job_from_payload,
)


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

        job = vm_job_from_payload(payload)

        self.assertEqual(job.id, "12345311")
        self.assertEqual(job.project_id, "project-1")
        self.assertTrue(job.is_vm)
        self.assertEqual(job.state, "IN_QUEUE")
        self.assertEqual(job.product_id, "cpu-amd-zen5-2-vcpu")
        self.assertEqual(job.cpu, 2)
        self.assertEqual(job.memory_gb, 6)
        self.assertEqual(job.disk_gb, 50)
        self.assertEqual(job.labels, {"ucloud-sandboxes/deployment": "prod-a"})
        self.assertEqual(job.private_network_ids, ("net-1",))
        self.assertEqual(job.queue_status, "FULL")
        self.assertFalse(job.ssh_enabled)


class HeartbeatContractTests(unittest.TestCase):
    def test_runtime_metrics_sanitize_nonfinite_and_malformed_values(self) -> None:
        metrics = NodeRuntimeMetrics.from_dict(
            {
                "collected_at": utc_now().isoformat(),
                "cpu_percent": "nan",
                "cpu_count": "invalid",
                "memory_total_mb": -1,
            }
        )

        self.assertIsNotNone(metrics)
        assert metrics is not None
        self.assertIsNone(metrics.cpu_percent)
        self.assertEqual(metrics.cpu_count, 0)
        self.assertEqual(metrics.memory_total_mb, 0)

    def test_untrusted_resource_values_are_sanitized_without_inflating_capacity(self) -> None:
        quantity = ResourceQuantity.from_dict(
            {"vcpu": "nan", "memory_mb": -1, "disk_mb": "invalid"}
        )

        self.assertEqual(quantity, ResourceQuantity())

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

    def test_future_legacy_timestamp_does_not_stay_fresh_forever(self) -> None:
        now = utc_now()
        heartbeat = NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            updated_at=now + timedelta(seconds=1),
            active_sandboxes=0,
        )

        self.assertFalse(heartbeat.is_fresh(now, ttl_seconds=10))

    def test_inventory_entry_round_trips_operation_identity(self) -> None:
        entry = SandboxInventoryEntry(
            sandbox_id="sandbox-1",
            generation=3,
            operation_id="operation-7",
            spec_hash="sha256:abc",
            state="running",
        )

        self.assertEqual(SandboxInventoryEntry.from_dict(entry.to_dict()), entry)

    def test_versioned_inventory_requires_complete_incarnation_identity(self) -> None:
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

        self.assertEqual(
            SandboxInventoryEntry.from_dict(
                {"sandbox_id": "legacy-sandbox", "generation": 0}
            ),
            SandboxInventoryEntry(sandbox_id="legacy-sandbox"),
        )

    def test_estimates_cpu_from_vm_product_id_when_resolved_product_is_absent(self) -> None:
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

        job = vm_job_from_payload(payload)

        self.assertEqual(job.cpu, 2)


if __name__ == "__main__":
    unittest.main()
