import unittest

from ucloud_sandboxes.capabilities import DISK_QUOTA_CAPABILITY
from ucloud_sandboxes.models import (
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    utc_now,
)
from ucloud_sandboxes.resource_admission import (
    dynamic_request_fits,
    node_accepts_dynamic_request,
    reserve_dynamic_resources,
    reusable_dynamic_resources,
)


class DynamicResourceAdmissionTests(unittest.TestCase):
    def heartbeat(self, **metric_overrides: object) -> NodeHeartbeat:
        metrics = {
            "collected_at": utc_now(),
            "cpu_percent": 10.0,
            "cpu_count": 32,
            "load_average_1m": 2.0,
            "memory_total_mb": 128_000,
            "memory_available_mb": 96_000,
        }
        metrics.update(metric_overrides)
        total = ResourceQuantity(vcpu=32, memory_mb=128_000, disk_mb=1_000_000)
        return NodeHeartbeat(
            node_id="node-1",
            job_id="job-1",
            updated_at=utc_now(),
            active_sandboxes=20,
            capabilities=(DISK_QUOTA_CAPABILITY,),
            total_resources=total,
            resources_known=True,
            runtime_metrics=NodeRuntimeMetrics(**metrics),
        )

    def test_cpu_and_memory_fit_node_shape_while_disk_is_reserved(self) -> None:
        total = ResourceQuantity(vcpu=32, memory_mb=128_000, disk_mb=1_000_000)
        available = ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=50_000)
        requested = ResourceQuantity(vcpu=8, memory_mb=16_000, disk_mb=10_000)

        self.assertTrue(dynamic_request_fits(requested, available, total))
        self.assertEqual(
            reserve_dynamic_resources(available, requested),
            ResourceQuantity(vcpu=1, memory_mb=512, disk_mb=40_000),
        )
        self.assertEqual(
            reusable_dynamic_resources(available, total),
            ResourceQuantity(vcpu=32, memory_mb=128_000, disk_mb=50_000),
        )

    def test_live_pressure_is_the_cpu_and_memory_admission_authority(self) -> None:
        request = ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=10_000)
        available = ResourceQuantity(vcpu=0, memory_mb=0, disk_mb=50_000)

        self.assertTrue(
            node_accepts_dynamic_request(self.heartbeat(), request, available)
        )
        self.assertFalse(
            node_accepts_dynamic_request(
                self.heartbeat(cpu_percent=95.0),
                request,
                available,
            )
        )
        self.assertFalse(
            node_accepts_dynamic_request(
                self.heartbeat(memory_available_mb=1024),
                request,
                available,
            )
        )

    def test_request_must_still_fit_physical_shape_and_hard_disk(self) -> None:
        heartbeat = self.heartbeat()
        available = ResourceQuantity(disk_mb=10_000)

        self.assertFalse(
            node_accepts_dynamic_request(
                heartbeat,
                ResourceQuantity(vcpu=33, memory_mb=1024, disk_mb=1),
                available,
            )
        )
        self.assertFalse(
            node_accepts_dynamic_request(
                heartbeat,
                ResourceQuantity(vcpu=1, memory_mb=1024, disk_mb=10_001),
                available,
            )
        )


if __name__ == "__main__":
    unittest.main()
