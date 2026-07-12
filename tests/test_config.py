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
                    "cpu_overcommit": 2.0,
                    "memory_overcommit": 1.2,
                    "disk_overcommit": 1.0,
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
        self.assertEqual(config.policy.cpu_overcommit, 2.0)
        self.assertEqual(config.policy.memory_overcommit, 1.2)
        self.assertEqual(config.policy.disk_overcommit, 1.0)
        self.assertEqual(config.policy.schedulable_node_resources.vcpu, 64)
        self.assertEqual(config.policy.schedulable_node_resources.memory_mb, 78643)
        self.assertEqual(config.gateway_public_link_id, "12345368")
        self.assertEqual(config.gateway_public_link_port, 8090)
        self.assertEqual(config.metrics_file, "/tmp/ucloud-sandboxes-metrics.jsonl")

    def test_rejects_invalid_policy_numbers_and_impossible_ranges(self) -> None:
        invalid_configs = {
            "negative minimum": {"policy": {"min_nodes": -1}},
            "minimum exceeds maximum": {
                "policy": {"min_nodes": 3, "max_nodes": 2}
            },
            "nan capacity weight": {
                "policy": {"provisioning_capacity_weight": "nan"}
            },
            "infinite capacity weight": {
                "policy": {"stale_provisioning_capacity_weight": "inf"}
            },
            "weight above one": {
                "policy": {"provisioning_capacity_weight": 1.1}
            },
            "zero heartbeat ttl": {"policy": {"heartbeat_ttl_seconds": 0}},
            "negative warm resources": {
                "policy": {"warm_resources": {"vcpu": -1}}
            },
            "nonintegral memory": {
                "policy": {"warm_resources": {"memory_mb": 1.5}}
            },
            "zero default resources": {
                "policy": {
                    "default_node_resources": {
                        "vcpu": 1,
                        "memory_mb": 1024,
                        "disk_mb": 0,
                    }
                }
            },
            "zero cpu overcommit": {"policy": {"cpu_overcommit": 0}},
            "nan memory overcommit": {"policy": {"memory_overcommit": "nan"}},
            "invalid public link port": {"gateway_public_link_port": 70000},
        }

        for label, raw in invalid_configs.items():
            with self.subTest(label=label), self.assertRaises(ValueError):
                AutoscalerConfig.from_dict(raw)

    def test_rejects_boolean_for_integer_policy_field(self) -> None:
        with self.assertRaises(ValueError):
            AutoscalerConfig.from_dict({"policy": {"max_nodes": True}})


if __name__ == "__main__":
    unittest.main()
