import json
from pathlib import Path
import sys
import tarfile
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
        self.assertEqual(
            gateway["UCLOUD_HEARTBEAT_TOKEN_FILE"],
            "/work/ucloud-sandboxes/state/heartbeat-token",
        )
        self.assertEqual(
            gateway["UCLOUD_NODE_CONTROL_TOKEN_FILE"],
            "/work/ucloud-sandboxes/state/node-control-token",
        )
        self.assertEqual(gateway["UCLOUD_REGISTRY_URL"], "http://127.0.0.1:5000")
        self.assertEqual(registry["UCLOUD_REGISTRY_RETENTION_DAYS"], "30")
        self.assertEqual(registry["UCLOUD_REGISTRY_KEEP_PER_REPOSITORY"], "0")
        self.assertEqual(
            registry["UCLOUD_REGISTRY_USAGE_FILE"],
            "/work/ucloud-sandboxes/state/registry-usage.json",
        )
        self.assertEqual(
            registry["UCLOUD_IMAGE_FILE"],
            "/work/ucloud-sandboxes/state/images.json",
        )
        self.assertEqual(
            autoscaler["UCLOUD_INIT_HEARTBEAT_URL"],
            "http://sandbox-gateway-prod:8090/v1/nodes/heartbeat",
        )
        self.assertEqual(
            autoscaler["UCLOUD_INIT_HEARTBEAT_TOKEN_SOURCE_FILE"],
            "/work/ucloud-sandboxes/state/heartbeat-token",
        )
        self.assertEqual(
            autoscaler["UCLOUD_INIT_NODE_CONTROL_TOKEN_SOURCE_FILE"],
            "/work/ucloud-sandboxes/state/node-control-token",
        )
        self.assertEqual(
            autoscaler["UCLOUD_INIT_PACKAGE_SPEC"],
            "/work/ucloud-sandboxes/release/"
            "ucloud_sandboxes-0.2.0-py3-none-any-node-package.tar.gz",
        )
        self.assertEqual(autoscaler["UCLOUD_MAX_INIT_PER_CYCLE"], "4")
        self.assertEqual(
            autoscaler["UCLOUD_DOCKER_HOST_ALIAS"],
            "ucloud-sandbox-registry=10.0.0.5",
        )
        self.assertIn("/etc/ucloud-sandboxes/gateway.env", script)
        self.assertIn("NODE_PACKAGE_BUNDLE=", script)
        self.assertIn("pip\" download --disable-pip-version-check", script)
        self.assertIn("package-bundle.json", script)
        self.assertIn("gzip.GzipFile", script)
        self.assertIn("compresslevel=1", script)
        self.assertIn('Dir::State::status="$status_file"', script)
        self.assertIn('Dir::Cache::archives="$archive_dir"', script)
        self.assertIn("download_runtime_packages apt-transport-https", script)
        self.assertIn("docker pull busybox", script)
        self.assertIn("docker save --output", script)
        self.assertIn("'reference': 'busybox'", script)
        self.assertIn("'architecture': sys.argv[9]", script)
        self.assertIn("'sha256': sha256_file(path)", script)
        self.assertIn("mode='w|'", script)
        self.assertIn(
            "could not build offline Docker/gVisor bundle; cold nodes will use repository fallback",
            script,
        )
        self.assertIn("ucloud-sandbox-autoscaler.service", script)
        self.assertIn("ucloud-sandbox-registry-prune.timer", script)
        self.assertIn("systemctl enable --now ucloud-sandbox-registry-prune.timer", script)
        self.assertIn("curl -fsS http://127.0.0.1:8090/healthz", script)
        self.assertIn(
            "create_secret /work/ucloud-sandboxes/state/gateway-token",
            script,
        )
        self.assertIn(
            "create_secret /work/ucloud-sandboxes/state/heartbeat-token",
            script,
        )
        self.assertIn(
            "create_secret /work/ucloud-sandboxes/state/node-control-token",
            script,
        )
        self.assertEqual(
            len(
                {
                    plan.gateway_token_file,
                    plan.heartbeat_token_file,
                    plan.node_control_token_file,
                }
            ),
            3,
        )
        self.assertNotIn(
            "gateway-token /work/ucloud-sandboxes/state/heartbeat-token",
            script,
        )

    def test_offline_bundle_builder_python_compiles(self) -> None:
        with TemporaryDirectory() as raw_dir:
            wheel = Path(raw_dir) / "ucloud_sandboxes-0.2.0-py3-none-any.whl"
            wheel.write_bytes(b"wheel")
            script = render_remote_deploy_script(
                AllInOneDeployPlan(
                    job_id="job-1",
                    project_id="project-1",
                    deployment_id="prod-a",
                    local_wheel=wheel,
                    gateway_private_host="sandbox-gateway-prod",
                    registry_private_ip="10.0.0.5",
                    private_network_id="net-1",
                )
            )

        start = script.index("import hashlib\nimport gzip")
        end = script.index('\nPY\nrm -rf "$NODE_PACKAGE_WORK"', start)
        compile(script[start:end], "<offline-bundle-builder>", "exec")

    def test_offline_bundle_builder_records_platform_and_is_deterministic(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            wheel_dir = root / "wheels"
            wheel_dir.mkdir()
            wheel = wheel_dir / "ucloud_sandboxes-0.2.0-py3-none-any.whl"
            wheel.write_bytes(b"wheel")
            runtime_dir = root / "runtime"
            package_dir = runtime_dir / "debs"
            package_dir.mkdir(parents=True)
            (package_dir / "docker-ce_1.0_amd64.deb").write_bytes(b"docker")
            (package_dir / "runsc_1.0_amd64.deb").write_bytes(b"gvisor")
            image_dir = runtime_dir / "images"
            image_dir.mkdir()
            (image_dir / "runtime-conformance-busybox.tar").write_bytes(b"image")
            (image_dir / "runtime-conformance-busybox.inspect.json").write_text(
                json.dumps(
                    [{"Id": "sha256:image", "Os": "linux", "Architecture": "amd64"}]
                ),
                encoding="utf-8",
            )
            plan = AllInOneDeployPlan(
                job_id="job-1",
                project_id="project-1",
                deployment_id="prod-a",
                local_wheel=wheel,
                gateway_private_host="sandbox-gateway-prod",
                registry_private_ip="10.0.0.5",
                private_network_id="net-1",
            )
            script = render_remote_deploy_script(plan)
            start = script.index("import hashlib\nimport gzip")
            end = script.index('\nPY\nrm -rf "$NODE_PACKAGE_WORK"', start)
            code = compile(
                script[start:end],
                "<offline-bundle-builder>",
                "exec",
            )
            targets = (root / "first.tar.gz", root / "second.tar.gz")
            original_argv = sys.argv
            try:
                for target in targets:
                    sys.argv = [
                        "builder",
                        str(wheel),
                        str(wheel_dir),
                        str(target),
                        str(runtime_dir),
                        "1",
                        "ubuntu",
                        "24.04",
                        "noble",
                        "amd64",
                    ]
                    exec(code, {"__name__": "__main__"})
            finally:
                sys.argv = original_argv

            self.assertEqual(targets[0].read_bytes(), targets[1].read_bytes())
            with tarfile.open(targets[0], mode="r:gz") as archive:
                manifest_file = archive.extractfile("package-bundle.json")
                assert manifest_file is not None
                manifest = json.loads(manifest_file.read())

        self.assertEqual(
            manifest["runtime"]["platform"],
            {
                "os_id": "ubuntu",
                "version_id": "24.04",
                "codename": "noble",
                "architecture": "amd64",
            },
        )
        self.assertIn("docker-ce", manifest["runtime"]["packages"])
        self.assertIn("runsc", manifest["runtime"]["packages"])
        self.assertEqual(
            [item["name"] for item in manifest["runtime"]["files"]],
            ["docker-ce_1.0_amd64.deb", "runsc_1.0_amd64.deb"],
        )
        self.assertEqual(manifest["runtime"]["probe_image"]["reference"], "busybox")
        self.assertEqual(manifest["runtime"]["probe_image"]["image_id"], "sha256:image")

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
        self.assertIn("--usage-file", units["ucloud-sandbox-registry-prune.service"])
        self.assertIn("--image-file", units["ucloud-sandbox-registry-prune.service"])
        self.assertIn(
            "--prune-stale-image-records",
            units["ucloud-sandbox-registry-prune.service"],
        )
        self.assertIn(
            "flock --exclusive --nonblock",
            units["ucloud-sandbox-registry-prune.service"],
        )
        self.assertNotIn(
            "ExecStartPost",
            units["ucloud-sandbox-registry-prune.service"],
        )
        self.assertIn(
            "/work/data/ucloud-sandbox-registry/docker-registry",
            units["ucloud-sandbox-registry-gc.service"],
        )
        self.assertIn(
            "-m ucloud_sandboxes.systemd registry-gc",
            units["ucloud-sandbox-registry-gc.service"],
        )
        self.assertIn(
            "--init-heartbeat-bearer-token-source-file ${UCLOUD_INIT_HEARTBEAT_TOKEN_SOURCE_FILE}",
            units["ucloud-sandbox-autoscaler.service"],
        )
        self.assertIn(
            "--heartbeat-bearer-token-file ${UCLOUD_HEARTBEAT_TOKEN_FILE}",
            units["ucloud-sandbox-gateway.service"],
        )
        self.assertIn(
            "--node-control-bearer-token-file ${UCLOUD_NODE_CONTROL_TOKEN_FILE}",
            units["ucloud-sandbox-gateway.service"],
        )
        self.assertIn(
            "--init-node-control-bearer-token-source-file ${UCLOUD_INIT_NODE_CONTROL_TOKEN_SOURCE_FILE}",
            units["ucloud-sandbox-autoscaler.service"],
        )
        self.assertIn(
            "--init-buildx-direct-push",
            units["ucloud-sandbox-autoscaler.service"],
        )
        self.assertIn("EnvironmentFile=/etc/ucloud-sandboxes/gateway.env", units["ucloud-sandbox-gateway.service"])


if __name__ == "__main__":
    unittest.main()
