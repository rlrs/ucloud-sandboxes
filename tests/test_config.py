import unittest

from ucloud_sandboxes.config import AutoscalerConfig


class ConfigTests(unittest.TestCase):
    def test_parses_slow_start_policy_fields(self) -> None:
        config = AutoscalerConfig.from_dict(
            {
                "project_id": "project-1",
                "policy": {
                    "warm_resources": {
                        "vcpu": 4,
                        "memory_mb": 8192,
                        "disk_mb": 51200,
                    },
                    "max_provisioning_nodes": 3,
                    "provisioning_capacity_weight": 0.5,
                    "stale_provisioning_after_seconds": 600,
                    "stale_provisioning_capacity_weight": 0.1,
                    "scale_down_idle_seconds": 900,
                },
                "gateway_public_link_id": "12345368",
                "gateway_public_link_port": 8090,
                "metrics_file": "/tmp/ucloud-sandboxes-metrics.jsonl",
            }
        )

        self.assertEqual(config.policy.warm_resources.vcpu, 4)
        self.assertEqual(config.policy.warm_resources.memory_mb, 8192)
        self.assertEqual(config.policy.max_provisioning_nodes, 3)
        self.assertEqual(config.policy.provisioning_capacity_weight, 0.5)
        self.assertEqual(config.policy.stale_provisioning_after_seconds, 600)
        self.assertEqual(config.policy.stale_provisioning_capacity_weight, 0.1)
        self.assertEqual(config.policy.scale_down_idle_seconds, 900)
        self.assertEqual(config.gateway_public_link_id, "12345368")
        self.assertEqual(config.gateway_public_link_port, 8090)
        self.assertEqual(config.metrics_file, "/tmp/ucloud-sandboxes-metrics.jsonl")


if __name__ == "__main__":
    unittest.main()
