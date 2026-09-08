import unittest
from dataclasses import replace

from ucloud_sandboxes.capabilities import DISK_QUOTA_CAPABILITY
from ucloud_sandboxes.models import (
    NodeHeartbeat,
    NodeRuntimeMetrics,
    ResourceQuantity,
    utc_now,
)
from ucloud_sandboxes.resource_admission import (
    dynamic_cpu_pressure_retryable,
    dynamic_pressure_error,
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

    def test_free_swap_cannot_hide_exhausted_physical_memory(self) -> None:
        heartbeat = self.heartbeat(
            memory_available_mb=2047,
            swap_total_mb=96 * 1024,
            swap_free_mb=96 * 1024,
            memory_psi_full_avg10=0.0,
        )
        # Exec uses a zero-resource lease; create/wake supply their limits.
        # Both must stop before lagging PSI reports reclaim thrashing.
        for request in (
            ResourceQuantity(),
            ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=1024),
        ):
            with self.subTest(request=request):
                self.assertIn(
                    "physical live memory headroom",
                    dynamic_pressure_error(heartbeat.runtime_metrics, request) or "",
                )

    def test_io_inflated_load_requires_live_cpu_corroboration(self) -> None:
        for request in (ResourceQuantity(), ResourceQuantity(vcpu=4, memory_mb=4096)):
            for cpu_percent, load, error in (
                (10.0, 64.0, None),
                (79.9, 64.0, None),
                (80.0, 64.0, "CPU load"),
                (85.0, 64.0, "CPU load"),
                (95.0, 0.0, "CPU pressure"),
                (None, 64.0, "CPU load"),
            ):
                with self.subTest(request=request, cpu=cpu_percent, load=load):
                    observed = dynamic_pressure_error(
                        self.heartbeat(
                            cpu_percent=cpu_percent, load_average_1m=load
                        ).runtime_metrics,
                        request,
                    )
                    if error is None:
                        self.assertIsNone(observed)
                    else:
                        self.assertIn(error, observed or "")

    def test_only_known_cpu_pressure_without_memory_failure_can_wait(self) -> None:
        request = ResourceQuantity(vcpu=4, memory_mb=8192)
        for overrides, expected in (
            ({"cpu_percent": 95.0}, True),
            ({"cpu_percent": 80.0, "load_average_1m": 64.0}, True),
            ({"cpu_percent": 79.9, "load_average_1m": 64.0}, False),
            ({"cpu_percent": None, "load_average_1m": 64.0}, False),
            ({"cpu_percent": 95.0, "memory_psi_full_avg10": 10.0}, False),
            (
                {
                    "cpu_percent": 95.0,
                    "memory_available_mb": 2047,
                    "swap_total_mb": 100_000,
                    "swap_free_mb": 100_000,
                },
                False,
            ),
            ({"cpu_percent": 95.0, "memory_available_mb": 8191}, False),
        ):
            with self.subTest(overrides=overrides):
                self.assertEqual(
                    dynamic_cpu_pressure_retryable(
                        self.heartbeat(**overrides).runtime_metrics, request
                    ),
                    expected,
                )
        self.assertFalse(dynamic_cpu_pressure_retryable(None, request))

    def test_swap_can_still_absorb_cold_pages_above_physical_floor(self) -> None:
        request = ResourceQuantity(vcpu=4, memory_mb=8192, disk_mb=1024)
        heartbeat = self.heartbeat(
            memory_available_mb=2048,
            swap_total_mb=96 * 1024,
            swap_free_mb=6144,
        )
        self.assertTrue(
            node_accepts_dynamic_request(
                heartbeat, request, ResourceQuantity(disk_mb=1024)
            )
        )
        self.assertFalse(
            node_accepts_dynamic_request(
                self.heartbeat(
                    memory_available_mb=2048,
                    swap_total_mb=96 * 1024,
                    swap_free_mb=6143,
                ),
                request,
                ResourceQuantity(disk_mb=1024),
            )
        )

    def test_128_sandbox_limits_fit_32_vcpu_node_under_measured_headroom(self) -> None:
        total = ResourceQuantity(vcpu=32, memory_mb=96 * 1024, disk_mb=128 * 8192)
        available = total
        request = ResourceQuantity(vcpu=4, memory_mb=4096, disk_mb=8192)
        heartbeat = replace(
            self.heartbeat(
                memory_total_mb=total.memory_mb,
                memory_available_mb=48 * 1024,
                cpu_percent=50.0,
                load_average_1m=16.0,
            ),
            total_resources=total,
            active_sandboxes=128,
        )
        for _ in range(128):
            self.assertTrue(node_accepts_dynamic_request(heartbeat, request, available))
            available = reserve_dynamic_resources(available, request)
        # Configured limits exceed physical CPU/RAM; hard disk never does.
        self.assertEqual(available.disk_mb, 0)
        self.assertFalse(node_accepts_dynamic_request(heartbeat, request, available))

        pressured = replace(
            heartbeat,
            runtime_metrics=replace(heartbeat.runtime_metrics, cpu_percent=95.0),
        )
        self.assertFalse(node_accepts_dynamic_request(pressured, request, total))


if __name__ == "__main__":
    unittest.main()
