import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import ucloud_sandboxes.vm_init as vm_init
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.providers.ucloud.bootstrap import (
    bootstrap_access_from_payload,
    extract_ssh_command,
)
from ucloud_sandboxes.vm_init import (
    BUILDER_RUNTIME_PACKAGES,
    PINNED_STORAGE_NATIVE_AGENTENV_COMMIT,
    RUNTIME_KERNEL_MODULES,
    SANDBOX_RUNTIME_PACKAGES,
    VmInitOptions,
    parse_vm_init_phases,
    render_vm_init_script,
    stage_vm_init_package_over_ssh,
)

ARCHITECTURE = "amd64" if os.uname().machine == "x86_64" else "arm64"
HOST_ARCHITECTURE = "x86_64" if ARCHITECTURE == "amd64" else "aarch64"
RUNSC_COMMIT = "9f653e577965df2ddd13875b5530cd2588661f1c"
CORRUPTIONS = """\
m runtime.platform.os_id "debian"
m runtime.role "builder"
m runtime.packages []
f runtime/debs/runtime.deb
m runtime.agent.python "0.0"
f runtime/agent/node-agent-runtime.tar
m runtime.kernel.release "wrong"
f runtime/kernel/{release}/runtime.ko
m runtime.direct_runsc.commit "0"
f runtime/direct/runsc
f runtime/direct/ucloud-sandbox-init
m runtime.storage_native.host_architecture "wrong"
f runtime/storage-native/backend
f runtime/storage-native/build-manifest.json
f runtime/storage-native/LICENSE
b patch
""".format(release=os.uname().release).splitlines()


def write_bundle(root: Path, role: str) -> dict:
    def artifact(relative, content=None, *, basename=False, **values):
        content = content or relative.encode()
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return {"name" if basename else "file": path.name if basename else relative, "sha256": hashlib.sha256(content).hexdigest(), "size": len(content), **values}  # fmt: skip

    # fmt: off
    runtime = {
        "platform": {"os_id": "ubuntu", "version_id": "24.04", "codename": "noble", "architecture": ARCHITECTURE},
        "role": role,
        "packages": list(BUILDER_RUNTIME_PACKAGES if role == "builder" else SANDBOX_RUNTIME_PACKAGES),
        "files": [artifact("runtime/debs/runtime.deb", basename=True)],
        "agent": artifact("runtime/agent/node-agent-runtime.tar", python=f"{sys.version_info.major}.{sys.version_info.minor}"),
        "kernel": {
            "release": os.uname().release,
            "load": list(RUNTIME_KERNEL_MODULES),
            "files": [artifact(f"runtime/kernel/{os.uname().release}/runtime.ko", basename=True)],
        },
    }
    if role == "sandbox":
        backend = artifact("runtime/storage-native/backend")
        license_metadata = artifact("runtime/storage-native/LICENSE")
        patch_names = ("agentenv-streaming-dense-export.patch", "agentenv-pooled-delete.patch", "agentenv-owner-identity.patch")
        build = {  # fmt: skip
            "schema": 3, "agentenv_commit": PINNED_STORAGE_NATIVE_AGENTENV_COMMIT,
            "artifact_sha256": backend["sha256"], "host_architecture": HOST_ARCHITECTURE, "license": "MIT",
            "patches": [{"name": name, "sha256": character * 64} for name, character in zip(patch_names, "abc", strict=True)],
        }
        build_metadata = artifact("runtime/storage-native/build-manifest.json", (json.dumps(build) + "\n").encode())
        runtime.update(
            direct_runsc=artifact("runtime/direct/runsc", commit=RUNSC_COMMIT),
            managed_init=artifact("runtime/direct/ucloud-sandbox-init"),
            storage_native=backend | {
                "agentenv_commit": PINNED_STORAGE_NATIVE_AGENTENV_COMMIT, "host_architecture": HOST_ARCHITECTURE,
                "license_file": license_metadata["file"], "license_sha256": license_metadata["sha256"],
                "manifest_file": build_metadata["file"], "manifest_sha256": build_metadata["sha256"],
            },
        )
    # fmt: on
    manifest = {"version": 1, "runtime": runtime}
    (root / "package-bundle.json").write_text(json.dumps(manifest), encoding="utf-8")
    return manifest


class VmInitTests(unittest.TestCase):
    @staticmethod
    def _options(**overrides: object) -> VmInitOptions:
        values: dict[str, object] = {
            "job_id": "job-1",
            "heartbeat_url": "https://gateway.example/v1/nodes/heartbeat",
            "heartbeat_bearer_token_file": "/run/ucloud/heartbeat-token",
            "heartbeat_bearer_token": "heartbeat-secret",
            "node_control_bearer_token_file": "/run/ucloud/node-token",
            "node_control_bearer_token": "node-secret",
            "deployment_id": "test-deployment",
            "package_spec": "/tmp/node-package.tar.gz",
            "package_sha256": "a" * 64,
            "direct_runsc_commit": RUNSC_COMMIT,
            "storage_native_registry_url": "http://registry.internal:5000",
            "docker_quota_image_gb": 440,
            "total_resources": ResourceQuantity(
                vcpu=32,
                memory_mb=128 * 1024,
                disk_mb=2_000 * 1024,
            ),
        }
        values.update(overrides)
        return VmInitOptions(**values)

    def _bundle_validator(self, role="sandbox"):
        script = render_vm_init_script(self._options(role=role))
        start = script.index("import hashlib\nimport json\nimport os")
        end = script.index('\nPY\n)"', start)
        return script, script[start:end]

    @staticmethod
    def _run_bundle_validator(validator, root):
        return subprocess.run(
            [sys.executable, "-", str(root), "ubuntu", "24.04", "noble", ARCHITECTURE],
            input=validator,
            text=True,
            capture_output=True,
        )

    def test_sandbox_boot_uses_only_verified_bundle(self) -> None:
        script = render_vm_init_script(self._options(direct_network="sandbox"))
        present = (
            "serve-direct-node-agent|A staged node package bundle is required|Node package bundle checksum does not match|"
            "Verified pinned Docker/gVisor bundle|install_bundled_runtime|bundle-verified direct runsc|"
            "bundle-verified storage-native backend|Activating bundled ucloud-sandboxes runtime|"
            "--storage-native-socket|--volume-mount-root"
        )
        absent = "apt-get update|package repository|runtime-conformance|Preassembled runtime unavailable|installed-package.fingerprint|serve-node-agent|legacy"
        for expected in present.split("|"):
            self.assertIn(expected, script)
        for obsolete in absent.split("|"):
            self.assertNotIn(
                obsolete, script if obsolete != "package repository" else script.lower()
            )
        self.assertIn("Dir::Etc::sourcelist", script)
        self.assertNotIn("apt-get install --no-download", script)
        self.assertIn("--keep-directory-symlink", script)
        self.assertIn("-ef /usr/lib/systemd/system/containerd.service", script)
        self.assertIn("date +%s%N", script)
        self.assertNotIn("date +%s%3N", script)
        self.assertIn("Using snapshot-baked runtime", script)
        self.assertIn("UCLOUD_STATIC_RUNTIME_READY", script)
        self.assertIn("Recorded snapshot-ready runtime", script)
        self.assertIn("runtime-ready-v3-sandbox", script)
        self.assertNotIn("$UCLOUD_STATE_DIR/package-bundles", script)

    def test_docker_mtu_uses_smallest_routed_interface(self) -> None:
        script = render_vm_init_script(self._options())

        self.assertIn("detect_routed_mtu()", script)
        self.assertIn("ip -o route show table main", script)
        self.assertIn('[ "$iface_mtu" -lt "$mtu" ]', script)
        self.assertIn('UCLOUD_DOCKER_MTU="$(detect_routed_mtu)"', script)

    def test_storage_native_resize_cache_is_isolated_from_runtime_cache(self) -> None:
        script = render_vm_init_script(self._options())

        self.assertIn(
            "UCLOUD_STORAGE_NATIVE_RESIZE_BACKEND_CONFIG="
            "/etc/ucloud-sandboxes/storage-native-resize-backend.json",
            script,
        )
        self.assertIn('$UCLOUD_STORAGE_NATIVE_CACHE_ROOT/remote-blocks', script)
        self.assertIn('$UCLOUD_STORAGE_NATIVE_CACHE_ROOT/resize-blocks', script)
        self.assertIn(
            "--resize-global-config "
            "${UCLOUD_STORAGE_NATIVE_RESIZE_BACKEND_CONFIG}",
            script,
        )
        self.assertIn('"download": {"enable": False}', script)
        self.assertIn("--snapshot-compact-after-layers 8", script)
        self.assertIn("--snapshot-compact-after-bytes 4294967296", script)

    def test_builder_keeps_image_build_runtime(self) -> None:
        script = render_vm_init_script(
            self._options(
                role="builder",
                buildx_cache_ref="registry.internal:5000/cache/buildkit",
                docker_quota_image_gb=200,
            )
        )

        self.assertIn("serve-builder-agent", script)
        self.assertIn("--buildx-direct-push", script)
        self.assertIn(
            "--buildx-cache-ref registry.internal:5000/cache/buildkit", script
        )
        self.assertIn("SupplementaryGroups=docker", script)
        self.assertNotIn("ucloud-storage-native.service\nRestart=always", script)

    def test_bootstrap_auth_and_identity_are_mandatory(self) -> None:
        required = {
            "deployment_id": "deployment id",
            "heartbeat_bearer_token_file": "heartbeat bearer token file",
            "heartbeat_bearer_token": "heartbeat bearer token",
            "node_control_bearer_token_file": "node control bearer token file",
            "node_control_bearer_token": "node control bearer token",
            "package_sha256": "package sha256",
        }
        for field, message in required.items():
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, message):
                render_vm_init_script(self._options(**{field: ""}))

    def test_direct_runtime_requires_bounded_image_storage(self) -> None:
        with self.assertRaisesRegex(ValueError, "bounded Docker image"):
            render_vm_init_script(self._options(docker_quota_image_gb=0))

    def test_embedded_runtime_validator_and_shell_compile(self) -> None:
        script, validator = self._bundle_validator()
        compile(validator, "<bundle-validator>", "exec")
        self.assertEqual(script.count('UCLOUD_PACKAGE_METADATA="$(python3 -'), 1)
        self.assertEqual(
            script.count('(bundle_dir / "package-bundle.json").read_text'), 1
        )
        self.assertNotRegex(
            script,
            r"UCLOUD_(?:DIRECT_RUNSC|MANAGED_INIT|STORAGE_NATIVE|AGENT_RUNTIME)_SPEC",
        )
        markers = (
            "UCLOUD_PACKAGE_METADATA=|Installing Docker, gVisor, and host support|"
            "Installing bundled container-runtime kernel module closure|Installing bundle-verified direct runsc runtime|"
            "Activating bundled ucloud-sandboxes runtime|Writing node-agent systemd service"
        ).split("|")
        self.assertEqual(
            [script.index(item) for item in markers],
            sorted(script.index(item) for item in markers),
        )

        syntax = subprocess.run(
            ["bash", "-n"],
            input=script,
            text=True,
            capture_output=True,
        )
        self.assertEqual(syntax.returncode, 0, syntax.stderr)

    def test_bundle_validator_returns_exact_role_metadata(self) -> None:
        for role in ("sandbox", "builder"):
            with self.subTest(role=role), TemporaryDirectory() as raw_dir:
                _script, validator = self._bundle_validator(role)
                root = Path(raw_dir)
                manifest = write_bundle(root, role)
                runtime = manifest["runtime"]
                completed = self._run_bundle_validator(validator, root)
                expected = [runtime["agent"]["sha256"]]
                if role == "sandbox":
                    expected.extend(
                        runtime[name]["sha256"]
                        for name in "direct_runsc managed_init storage_native".split()
                    )
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertEqual(completed.stdout.strip().split("\t"), expected)

    def test_bundle_validator_fails_closed_on_provenance_corruption(self) -> None:
        _script, validator = self._bundle_validator()
        self.assertEqual(len(CORRUPTIONS), 16)
        for corruption in CORRUPTIONS:
            with self.subTest(corruption=corruption), TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                manifest = write_bundle(root, "sandbox")
                kind, target, *raw = corruption.split(maxsplit=2)
                if kind == "f":
                    path = root / target
                    path.write_bytes(bytes(value ^ 0xFF for value in path.read_bytes()))
                elif kind == "b":
                    build_path = root / "runtime/storage-native/build-manifest.json"
                    build = json.loads(build_path.read_text(encoding="utf-8"))
                    build["patches"][0]["name"] = "wrong.patch"
                    content = (json.dumps(build) + "\n").encode()
                    build_path.write_bytes(content)
                    manifest["runtime"]["storage_native"]["manifest_sha256"] = (
                        hashlib.sha256(content).hexdigest()
                    )
                else:
                    keys = target.split(".")
                    container = manifest
                    for key in keys[:-1]:
                        container = container[key]
                    value = json.loads(raw[0])
                    container[keys[-1]] = (
                        value * 40 if target.endswith("commit") else value
                    )
                (root / "package-bundle.json").write_text(
                    json.dumps(manifest), encoding="utf-8"
                )
                completed = self._run_bundle_validator(validator, root)
                self.assertNotEqual(completed.returncode, 0)

    def test_runtime_module_closure_keeps_project_mounts_rebootable(self) -> None:
        self.assertIn("virtiofs", RUNTIME_KERNEL_MODULES)
        self.assertIn("ublk_drv", RUNTIME_KERNEL_MODULES)

    def test_plans_only_running_vm_with_ssh(self) -> None:
        payload = {
            "id": "123",
            "status": {"state": "RUNNING"},
            "updates": [{"status": "SSH Access: ssh ucloud@example -p 22"}],
        }
        plan = bootstrap_access_from_payload(payload)
        self.assertTrue(plan.runnable)
        self.assertEqual(plan.command, "ssh ucloud@example -p 22")
        self.assertEqual(extract_ssh_command(payload), plan.command)

        payload["status"]["state"] = "IN_QUEUE"
        self.assertFalse(bootstrap_access_from_payload(payload).runnable)

    def test_parses_machine_readable_phase_timings(self) -> None:
        phases, total = parse_vm_init_phases(
            "UCLOUD_INIT_PHASE name=runtime-bundle duration_ms=17321 total_ms=19002\n"
            "UCLOUD_INIT_PHASE name=docker-daemon duration_ms=823 total_ms=24100\n"
        )
        self.assertEqual(phases, {"runtime-bundle": 17321, "docker-daemon": 823})
        self.assertEqual(total, 24100)

    def test_stages_bundle_with_digest(self) -> None:
        calls: list[tuple[tuple[str, ...], bytes | None]] = []

        def fake_run(command, *, stdin=None, check=None, timeout=None):
            del check, timeout
            body = stdin.read() if stdin is not None else None
            calls.append((tuple(command), body))
            return subprocess.CompletedProcess(command, 1 if stdin is None else 0)

        with patch.object(
            vm_init.subprocess, "run", side_effect=fake_run
        ), TemporaryDirectory() as raw_dir:
            package = Path(raw_dir) / "node-package.tar.gz"
            package.write_bytes(b"verified-bundle")
            result = stage_vm_init_package_over_ssh(
                "ssh ucloud@example -p 22",
                self._options(package_spec=str(package)),
                timeout_seconds=10,
            )

        assert result is not None
        expected_digest = hashlib.sha256(b"verified-bundle").hexdigest()
        self.assertEqual(result.package_sha256, expected_digest)
        self.assertEqual(
            result.remote_path,
            "/var/cache/ucloud-sandboxes/init-packages/"
            f"{expected_digest}/node-package.tar.gz",
        )
        self.assertEqual(len(calls), 2)
        self.assertIn("runtime-ready-v3-sandbox", " ".join(calls[0][0]))
        self.assertEqual(calls[1][1], b"verified-bundle")

    def test_snapshot_ready_receipt_skips_bundle_transfer(self) -> None:
        calls: list[tuple[str, ...]] = []

        def fake_run(command, *, stdin=None, check=None, timeout=None):
            del check, timeout
            self.assertIsNone(stdin)
            calls.append(tuple(command))
            return subprocess.CompletedProcess(command, 0)

        with patch.object(
            vm_init.subprocess, "run", side_effect=fake_run
        ), TemporaryDirectory() as raw_dir:
            package = Path(raw_dir) / "node-package.tar.gz"
            package.write_bytes(b"snapshot-baked-bundle")
            result = stage_vm_init_package_over_ssh(
                "ssh root@10.42.0.2",
                self._options(package_spec=str(package)),
            )

        self.assertTrue(result.reused)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(len(calls), 1)
        self.assertIn("runtime-ready-v3-sandbox", " ".join(calls[0]))
