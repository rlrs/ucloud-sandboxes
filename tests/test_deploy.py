from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.deploy import (
    AllInOneDeployPlan,
    AUTO_REGISTRY_PRIVATE_IP_TOKEN,
    autoscaler_env,
    gateway_env,
    packaged_systemd_units,
    registry_env,
    render_env_file,
    render_remote_deploy_script,
)


class DeployTests(unittest.TestCase):
    def test_env_rendering_quotes_only_when_needed(self) -> None:
        text = render_env_file({"A": "plain-value", "B": "two words"})

        self.assertIn("A=plain-value\n", text)
        self.assertIn('B="two words"\n', text)

    def test_all_in_one_plan_renders_env_and_script(self) -> None:
        with TemporaryDirectory() as raw_dir:
            wheel = Path(raw_dir) / "ucloud_sandboxes-0.2.0-py3-none-any.whl"
            wheel.write_bytes(b"wheel")
            plan = AllInOneDeployPlan(
                job_id="job-1",
                project_id="project-1",
                deployment_id="prod-a",
                local_wheel=wheel,
                gateway_private_host="sandbox-gateway-prod",
                registry_private_ip="10.0.0.5",
                private_network_id="net-1",
            )

            gateway = gateway_env(plan)
            autoscaler = autoscaler_env(plan)
            registry = registry_env(plan)
            script = render_remote_deploy_script(plan)

        self.assertEqual(gateway["UCLOUD_DEPLOYMENT_ID"], "prod-a")
        self.assertEqual(gateway["UCLOUD_REGISTRY_URL"], "http://127.0.0.1:5000")
        self.assertEqual(registry["UCLOUD_REGISTRY_RETENTION_DAYS"], "3")
        self.assertEqual(registry["UCLOUD_REGISTRY_KEEP_PER_REPOSITORY"], "0")
        self.assertEqual(
            autoscaler["UCLOUD_INIT_HEARTBEAT_URL"],
            "http://sandbox-gateway-prod:8090/v1/nodes/heartbeat",
        )
        self.assertEqual(
            autoscaler["UCLOUD_DOCKER_HOST_ALIAS"],
            "ucloud-sandbox-registry=10.0.0.5",
        )
        self.assertIn("/etc/ucloud-sandboxes/gateway.env", script)
        self.assertIn("ucloud-sandbox-autoscaler.service", script)
        self.assertIn("ucloud-sandbox-registry-prune.timer", script)
        self.assertIn("systemctl enable --now ucloud-sandbox-registry-prune.timer", script)
        self.assertIn("curl -fsS http://127.0.0.1:8090/healthz", script)

    def test_all_in_one_plan_auto_detects_registry_private_ip(self) -> None:
        with TemporaryDirectory() as raw_dir:
            wheel = Path(raw_dir) / "ucloud_sandboxes-0.2.0-py3-none-any.whl"
            wheel.write_bytes(b"wheel")
            plan = AllInOneDeployPlan(
                job_id="job-1",
                project_id="project-1",
                deployment_id="prod-a",
                local_wheel=wheel,
                gateway_private_host="sandbox-gateway-prod",
                private_network_id="net-1",
            )

            autoscaler = autoscaler_env(plan)
            script = render_remote_deploy_script(plan)

        self.assertEqual(
            autoscaler["UCLOUD_DOCKER_HOST_ALIAS"],
            f"ucloud-sandbox-registry={AUTO_REGISTRY_PRIVATE_IP_TOKEN}",
        )
        self.assertIn("detect_registry_private_ip() {", script)
        self.assertIn("REGISTRY_PRIVATE_IP=''", script)
        self.assertIn(
            f"UCLOUD_DOCKER_HOST_ALIAS=ucloud-sandbox-registry={AUTO_REGISTRY_PRIVATE_IP_TOKEN}",
            script,
        )
        self.assertIn(
            f"sudo sed -i 's|{AUTO_REGISTRY_PRIVATE_IP_TOKEN}|",
            script,
        )

    def test_packaged_systemd_units_are_available(self) -> None:
        units = packaged_systemd_units()

        self.assertIn("ucloud-sandbox-gateway.service", units)
        self.assertIn("ucloud-sandbox-autoscaler.service", units)
        self.assertIn("ucloud-sandbox-registry-prune.service", units)
        self.assertIn("ucloud-sandbox-registry-prune.timer", units)
        self.assertIn("--max-age-days", units["ucloud-sandbox-registry-prune.service"])
        self.assertIn("EnvironmentFile=/etc/ucloud-sandboxes/gateway.env", units["ucloud-sandbox-gateway.service"])


if __name__ == "__main__":
    unittest.main()
