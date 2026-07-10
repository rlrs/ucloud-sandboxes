from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import ucloud_sandboxes.vm_init as vm_init
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.vm_init import (
    VmInitOptions,
    extract_ssh_command,
    extract_ssh_command_from_text,
    plan_vm_init,
    render_vm_init_script,
    stage_vm_init_package_over_ssh,
    ssh_init_command,
)


class VmInitTests(unittest.TestCase):
    def test_extracts_ssh_access_update(self) -> None:
        payload = {
            "updates": [
                {"status": "Starting"},
                {"status": "SSH Access: ssh ucloud@ssh.cloud.sdu.dk -p 41231"},
            ]
        }

        self.assertEqual(
            extract_ssh_command(payload),
            "ssh ucloud@ssh.cloud.sdu.dk -p 41231",
        )

    def test_extracts_available_at_update(self) -> None:
        self.assertEqual(
            extract_ssh_command_from_text(
                "SSH: ready\nSSH: Available at: ssh ucloud@ssh.cloud.sdu.dk -p 41231"
            ),
            "ssh ucloud@ssh.cloud.sdu.dk -p 41231",
        )

    def test_plans_runnable_vm_init(self) -> None:
        payload = {
            "id": "123",
            "specification": {
                "application": {"name": "vm-ubuntu", "version": "24.04"},
                "product": {"id": "cpu-amd-zen5-2-vcpu", "category": "cpu-amd-zen5"},
            },
            "status": {
                "state": "RUNNING",
                "jobParametersJson": {"request": {"sshEnabled": False}},
            },
            "updates": [{"status": "SSH Access: ssh ucloud@example -p 22"}],
        }

        plan = plan_vm_init(payload)

        self.assertTrue(plan.runnable)
        self.assertEqual(plan.ssh_command, "ssh ucloud@example -p 22")

    def test_plan_requires_announced_ssh_command(self) -> None:
        payload = {
            "id": "123",
            "status": {
                "state": "RUNNING",
                "jobParametersJson": {"request": {"sshEnabled": False}},
            },
            "updates": [{"status": "Your job is now running."}],
        }

        plan = plan_vm_init(payload)

        self.assertFalse(plan.runnable)
        self.assertIn("No SSH access command", plan.reason)

    def test_plan_waits_until_vm_is_running(self) -> None:
        payload = {
            "id": "123",
            "status": {
                "state": "IN_QUEUE",
                "jobParametersJson": {"request": {"sshEnabled": True}},
            },
            "updates": [{"status": "SSH Access: ssh ucloud@example -p 22"}],
        }

        plan = plan_vm_init(payload)

        self.assertFalse(plan.runnable)
        self.assertIn("not running", plan.reason)

    def test_renders_idempotent_init_script(self) -> None:
        script = render_vm_init_script(
            VmInitOptions(
                job_id="123",
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
                heartbeat_bearer_token_file="/work/ucloud-sandboxes/state/heartbeat-token",
                heartbeat_bearer_token="SECRET",
                node_control_bearer_token_file="/work/ucloud-sandboxes/state/node-control-token",
                node_control_bearer_token="NODE-SECRET",
                init_authorized_keys=("ssh-ed25519 AAAA gateway",),
                node_id="node-123",
                node_url="http://node-123:8090",
                package_spec="git+https://example.invalid/ucloud-sandboxes.git",
                total_resources=ResourceQuantity(vcpu=16, memory_mb=32768, disk_mb=500000),
                cpu_overcommit=4.0,
                docker_insecure_registries=("gateway:5000",),
                host_aliases=("ucloud-sandbox-registry=10.36.125.67",),
                enable_image_builds=True,
                labels={"pool": "builder"},
            )
        )

        self.assertIn("CONTAINER_PACKAGES+=(docker-ce docker-ce-cli", script)
        self.assertIn(
            'PACKAGES_TO_INSTALL=("${MISSING_BASE_PACKAGES[@]}" "${CONTAINER_PACKAGES[@]}")',
            script,
        )
        self.assertLess(
            script.index("Preparing Docker Engine repository"),
            script.index("Installing base and container packages"),
        )
        self.assertIn("xfsprogs", script)
        self.assertIn("https://storage.googleapis.com/gvisor/releases", script)
        self.assertIn("Init phase complete:", script)
        self.assertIn("UCLOUD_SERVICE_USER=ucloud", script)
        self.assertIn("UCLOUD_HEARTBEAT_BEARER_TOKEN=SECRET", script)
        self.assertIn("Installing heartbeat bearer token", script)
        self.assertIn("Installing node-control bearer token", script)
        self.assertIn("UCLOUD_NODE_CONTROL_BEARER_TOKEN=NODE-SECRET", script)
        self.assertIn("ssh-ed25519 AAAA gateway", script)
        self.assertIn("$UCLOUD_SERVICE_HOME/.ssh/authorized_keys", script)
        self.assertIn("$SUDO usermod -aG docker \"$UCLOUD_SERVICE_USER\"", script)
        self.assertIn("runuser -u \"$UCLOUD_SERVICE_USER\" -- \"$@\"", script)
        self.assertIn("run_as_service_user python3 -m venv \"$UCLOUD_VENV_DIR\"", script)
        self.assertNotIn("$SUDO python3 -m venv \"$UCLOUD_VENV_DIR\"", script)
        self.assertIn("UCLOUD_DOCKER_QUOTA_IMAGE_GB=200", script)
        self.assertIn("UCLOUD_DOCKER_MTU=0", script)
        self.assertIn("UCLOUD_DOCKER_DATA_ROOT=/var/lib/ucloud-sandboxes/docker", script)
        self.assertIn("UCLOUD_DOCKER_QUOTA_IMAGE=/var/lib/ucloud-sandboxes/docker-xfs.img", script)
        self.assertIn("UCLOUD_DOCKER_QUOTA_ROOT=/var/lib/ucloud-sandboxes/docker-xfs", script)
        self.assertNotIn("UCLOUD_DOCKER_QUOTA_IMAGE=/work/ucloud-sandboxes/docker-xfs.img", script)
        self.assertIn(
            "UCLOUD_HOST_ALIASES_JSON='[\"ucloud-sandbox-registry=10.36.125.67\"]'",
            script,
        )
        self.assertIn("Installing host aliases", script)
        self.assertIn("# ucloud-sandboxes host-alias ", script)
        self.assertIn(
            "UCLOUD_RUNTIME_CONFORMANCE_FILE=/work/ucloud-sandboxes/state/runtime-conformance.json",
            script,
        )
        self.assertIn("truncate -s \"${UCLOUD_DOCKER_QUOTA_IMAGE_GB}G\"", script)
        self.assertIn("mkfs.xfs -f -m reflink=1", script)
        self.assertIn("mount -o loop,pquota", script)
        self.assertIn('"data-root": os.environ["UCLOUD_DOCKER_DATA_ROOT"]', script)
        self.assertIn("cmp -s \"$DOCKER_DAEMON_JSON\" /etc/docker/daemon.json", script)
        self.assertIn("UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON='[\"gateway:5000\"]'", script)
        self.assertIn(
            "export RUNSC_PATH UCLOUD_DOCKER_DATA_ROOT UCLOUD_DOCKER_QUOTA_IMAGE_GB UCLOUD_DOCKER_MTU UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON",
            script,
        )
        self.assertIn("detect_default_route_mtu()", script)
        self.assertIn("ip -o route get 1.1.1.1", script)
        self.assertIn("Configuring Docker daemon with bridge MTU $UCLOUD_DOCKER_MTU", script)
        self.assertIn('docker_mtu = int(os.environ.get("UCLOUD_DOCKER_MTU") or "0")', script)
        self.assertIn('config["mtu"] = docker_mtu', script)
        self.assertIn('ip link set docker0 mtu "$UCLOUD_DOCKER_MTU"', script)
        self.assertIn("UCLOUD_DOCKER_MTU=$UCLOUD_DOCKER_MTU", script)
        self.assertIn('config["insecure-registries"] = insecure_registries', script)
        self.assertIn('config["storage-driver"] = "overlay2"', script)
        self.assertIn('"containerd-snapshotter": False', script)
        self.assertIn("runtime-conformance --sudo --execute --output json", script)
        self.assertIn("--disable-pip-version-check --upgrade \"$UCLOUD_PACKAGE_SPEC\"", script)
        self.assertIn("--runtime-conformance-file ${UCLOUD_RUNTIME_CONFORMANCE_FILE}", script)
        self.assertIn("ucloud-sandbox-node.service", script)
        self.assertIn("User=$UCLOUD_SERVICE_USER", script)
        self.assertIn("Group=$UCLOUD_SERVICE_GROUP", script)
        self.assertIn("SupplementaryGroups=docker", script)
        self.assertIn("ucloud-sandbox-heartbeat.timer", script)
        self.assertIn("systemctl restart ucloud-sandbox-node.service", script)
        self.assertIn("UCLOUD_NODE_AGENT_HOST=0.0.0.0", script)
        self.assertIn("UCLOUD_NODE_URL=http://node-123:8090", script)
        self.assertIn("--host ${UCLOUD_NODE_AGENT_HOST}", script)
        self.assertIn("--node-url ${UCLOUD_NODE_URL}", script)
        self.assertIn("--enable-image-builds --execute-runtime", script)
        self.assertIn(
            "agent-heartbeat --from-node-agent-url http://127.0.0.1:${UCLOUD_NODE_AGENT_PORT}",
            script,
        )
        self.assertIn(
            "--node-control-bearer-token-file ${UCLOUD_NODE_CONTROL_BEARER_TOKEN_FILE}",
            script,
        )
        self.assertIn(
            "--bearer-token-file ${UCLOUD_HEARTBEAT_BEARER_TOKEN_FILE}",
            script,
        )
        self.assertNotIn("--active 0", script)
        self.assertIn("--label pool=builder", script)

    def test_rendered_host_alias_python_compiles(self) -> None:
        script = render_vm_init_script(
            VmInitOptions(
                job_id="123",
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
                host_aliases=("ucloud-sandbox-registry=10.36.125.67",),
            )
        )
        start = script.index("import json\nimport os\nimport sys\n\nhosts_path")
        end = script.index("PY\n  $SUDO install -m 0644 \"$HOSTS_TMP\" /etc/hosts", start)
        compile(script[start:end], "<host-alias-heredoc>", "exec")

    def test_can_disable_quota_backed_docker_storage(self) -> None:
        script = render_vm_init_script(
            VmInitOptions(
                job_id="123",
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
                docker_quota_image_gb=0,
            )
        )

        self.assertIn("UCLOUD_DOCKER_QUOTA_IMAGE_GB=0", script)
        self.assertIn('if int(os.environ["UCLOUD_DOCKER_QUOTA_IMAGE_GB"]) > 0:', script)

    def test_can_override_docker_mtu(self) -> None:
        script = render_vm_init_script(
            VmInitOptions(
                job_id="123",
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
                docker_mtu=1400,
            )
        )

        self.assertIn("UCLOUD_DOCKER_MTU=1400", script)
        self.assertIn('if [ "$UCLOUD_DOCKER_MTU" -eq 0 ]; then', script)

    def test_runtime_dry_run_omits_execute_runtime(self) -> None:
        script = render_vm_init_script(
            VmInitOptions(
                job_id="123",
                heartbeat_url="https://control.example/v1/nodes/heartbeat",
                runtime_dry_run=True,
            )
        )

        self.assertNotIn("--execute-runtime", script)

    def test_stages_local_package_spec_over_ssh(self) -> None:
        calls: list[dict] = []

        class Completed:
            returncode = 0

        def fake_run(
            command,
            *,
            input=None,
            check=None,
            timeout=None,
        ):
            calls.append(
                {
                    "command": command,
                    "input": input,
                    "check": check,
                    "timeout": timeout,
                }
            )
            return Completed()

        original = vm_init.subprocess.run
        vm_init.subprocess.run = fake_run
        try:
            with TemporaryDirectory() as raw_dir:
                package = Path(raw_dir) / "ucloud_sandboxes-0.1.0-py3-none-any.whl"
                package.write_bytes(b"wheel-bytes")

                result = stage_vm_init_package_over_ssh(
                    "ssh ucloud@ssh.cloud.sdu.dk -p 41231",
                    VmInitOptions(
                        job_id="job-1",
                        heartbeat_url="https://control.example/v1/nodes/heartbeat",
                        package_spec=str(package),
                    ),
                    timeout_seconds=10,
                    private_key_file="/work/ucloud-sandboxes/state/ssh/gateway-init",
                )
        finally:
            vm_init.subprocess.run = original

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(
            result.remote_path,
            "/tmp/ucloud-sandboxes-init-packages/job-1/ucloud_sandboxes-0.1.0-py3-none-any.whl",
        )
        self.assertEqual(calls[0]["input"], b"wheel-bytes")
        self.assertEqual(calls[0]["timeout"], 10)
        self.assertIn("-i", calls[0]["command"])
        self.assertIn("cat > /tmp/ucloud-sandboxes-init-packages/job-1/", calls[0]["command"][-1])

    def test_builds_ssh_init_command(self) -> None:
        self.assertEqual(
            ssh_init_command(
                "ssh ucloud@ssh.cloud.sdu.dk -p 41231",
                private_key_file="/work/ucloud-sandboxes/state/ssh/gateway-init",
            ),
            (
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-i",
                "/work/ucloud-sandboxes/state/ssh/gateway-init",
                "ucloud@ssh.cloud.sdu.dk",
                "-p",
                "41231",
                "bash",
                "-s",
            ),
        )

    def test_rejects_non_ssh_command(self) -> None:
        with self.assertRaises(ValueError):
            ssh_init_command("curl https://example.invalid")

    def test_rejects_unsafe_service_user(self) -> None:
        with self.assertRaises(ValueError):
            render_vm_init_script(
                VmInitOptions(
                    job_id="123",
                    heartbeat_url="https://control.example/v1/nodes/heartbeat",
                    service_user="bad user",
                )
            )

    def test_rejects_multiline_init_authorized_key(self) -> None:
        with self.assertRaises(ValueError):
            render_vm_init_script(
                VmInitOptions(
                    job_id="123",
                    heartbeat_url="https://control.example/v1/nodes/heartbeat",
                    init_authorized_keys=("ssh-ed25519 AAAA\nssh-ed25519 BBBB",),
                )
            )

    def test_rejects_invalid_host_alias(self) -> None:
        with self.assertRaises(ValueError):
            render_vm_init_script(
                VmInitOptions(
                    job_id="123",
                    heartbeat_url="https://control.example/v1/nodes/heartbeat",
                    host_aliases=("ucloud-sandbox-registry",),
                )
            )

        with self.assertRaises(ValueError):
            render_vm_init_script(
                VmInitOptions(
                    job_id="123",
                    heartbeat_url="https://control.example/v1/nodes/heartbeat",
                    host_aliases=("bad host=10.36.125.67",),
                )
            )

    def test_rejects_negative_docker_mtu(self) -> None:
        with self.assertRaises(ValueError):
            render_vm_init_script(
                VmInitOptions(
                    job_id="123",
                    heartbeat_url="https://control.example/v1/nodes/heartbeat",
                    docker_mtu=-1,
                )
            )


if __name__ == "__main__":
    unittest.main()
