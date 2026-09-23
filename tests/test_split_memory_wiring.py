"""Rollout configuration and node accounting for opt-in split memory backing."""
from contextlib import redirect_stdout
from dataclasses import replace
from io import StringIO
from pathlib import Path
import subprocess
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from tests import test_direct_provisioner as direct_fixtures
from tests import test_vm_init as vm_fixtures
from ucloud_sandboxes import cli
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.direct_service import DirectSandboxService
from ucloud_sandboxes.models import NodeRuntimeMetrics, ResourceQuantity, utc_now
from ucloud_sandboxes.vm_init import render_vm_init_script


class SplitMemoryWiringTests(unittest.TestCase):
    def test_provider_bootstrap_uses_one_reflink_option_contract(self):
        from tests.test_policy import node
        base = DeploymentConfig.default(scope_id="test")
        base = replace(base, sandbox=replace(
            base.sandbox, direct_split_memory_backing=True,
            direct_ram_memory_backing=True, direct_reflink_memory_restore=True,
        ))
        for provider in ("ucloud", "hetzner"):
            config = replace(base, provider=replace(base.provider, kind=provider))
            with self.subTest(provider=provider), patch.object(
                cli, "read_bearer_token_source", return_value="test-token"
            ):
                for role in ("sandbox", "builder"):
                    options = cli.vm_init_options_for_job(
                        config, node("worker").job, role,
                        package_spec="/tmp/package.tar.gz", package_sha256="a" * 64,
                    )
                    self.assertEqual(options.direct_reflink_memory_restore, role == "sandbox")
                    self.assertEqual(cli.vm_init_options_to_dict(options)["directReflinkMemoryRestore"], role == "sandbox")
                    script = render_vm_init_script(options)
                    self.assertEqual(" --reflink-memory-restore" in script, role == "sandbox")

    def test_reflink_restore_is_explicit_and_requires_split(self):
        raw = DeploymentConfig.default(scope_id="test").to_dict()
        raw["sandbox"].pop("direct_reflink_memory_restore")
        self.assertFalse(DeploymentConfig.from_dict(raw).sandbox.direct_reflink_memory_restore)
        raw["sandbox"]["direct_reflink_memory_restore"] = True
        with self.assertRaisesRegex(ValueError, "requires split"):
            DeploymentConfig.from_dict(raw)
        raw["sandbox"]["direct_split_memory_backing"] = True
        for ram in (False, True):
            raw["sandbox"]["direct_ram_memory_backing"] = ram
            self.assertTrue(DeploymentConfig.from_dict(raw).sandbox.direct_reflink_memory_restore)
            script = render_vm_init_script(vm_fixtures.VmInitTests._options(
                direct_split_memory_backing=True, direct_ram_memory_backing=ram,
                direct_reflink_memory_restore=True,
            ))
            self.assertIn(" --reflink-memory-restore", script)
            self.assertIn("UCLOUD_DIRECT_REFLINK_MEMORY_RESTORE=1", script)
            self.assertEqual(" --application-memory-root ${UCLOUD_APPLICATION_MEMORY_ROOT}" in script, ram)
            syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True)
            self.assertEqual(syntax.returncode, 0, syntax.stderr)
        for value in (1, "true", None):
            raw["sandbox"]["direct_reflink_memory_restore"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                DeploymentConfig.from_dict(raw)
        with self.assertRaisesRegex(ValueError, "requires split"):
            render_vm_init_script(vm_fixtures.VmInitTests._options(direct_reflink_memory_restore=True))
        legacy = render_vm_init_script(vm_fixtures.VmInitTests._options())
        self.assertNotIn(" --reflink-memory-restore", legacy)

    def test_ram_backing_requires_split_and_survives_bootstrap_restart(self):
        raw = DeploymentConfig.default(scope_id="test").to_dict()
        raw["sandbox"].pop("direct_ram_memory_backing")
        self.assertFalse(DeploymentConfig.from_dict(raw).sandbox.direct_ram_memory_backing)
        raw["sandbox"]["direct_ram_memory_backing"] = True
        with self.assertRaises(ValueError):
            DeploymentConfig.from_dict(raw)
        raw["sandbox"]["direct_split_memory_backing"] = True
        self.assertTrue(DeploymentConfig.from_dict(raw).sandbox.direct_ram_memory_backing)
        script = render_vm_init_script(vm_fixtures.VmInitTests._options(
            direct_split_memory_backing=True, direct_ram_memory_backing=True,
        ))
        self.assertIn(" --application-memory-root ${UCLOUD_APPLICATION_MEMORY_ROOT}", script)
        self.assertIn("--ram-capacity-bytes", script)
        self.assertIn("UCLOUD_DIRECT_RAM_MEMORY_BACKING=1", script)
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        with self.assertRaises(ValueError):
            render_vm_init_script(vm_fixtures.VmInitTests._options(direct_ram_memory_backing=True))

    def test_old_configuration_stays_disabled_and_opt_in_is_boolean(self):
        raw = DeploymentConfig.default(scope_id="test").to_dict()
        raw["sandbox"].pop("direct_split_memory_backing")
        self.assertFalse(DeploymentConfig.from_dict(raw).sandbox.direct_split_memory_backing)
        raw["sandbox"]["direct_split_memory_backing"] = True
        self.assertTrue(DeploymentConfig.from_dict(raw).sandbox.direct_split_memory_backing)
        for value in ("false", 1, None):
            raw["sandbox"]["direct_split_memory_backing"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                DeploymentConfig.from_dict(raw)

    def test_bootstrap_opt_in_uses_shared_budget_registry_and_prepared_filesystem(self):
        options = vm_fixtures.VmInitTests._options
        legacy = render_vm_init_script(options())
        self.assertIn("UCLOUD_DIRECT_SPLIT_MEMORY_BACKING=0", legacy)
        self.assertNotIn(" --split-memory-backing", legacy)
        split = render_vm_init_script(options(direct_split_memory_backing=True))
        self.assertIn("UCLOUD_DIRECT_SPLIT_MEMORY_BACKING=1", split)
        self.assertIn(" --memory-backing-hard-capacity-bytes ${UCLOUD_STORAGE_NATIVE_HARD_CAPACITY_BYTES}", split)
        self.assertIn(" --checkpoint-registry-url ${UCLOUD_STORAGE_NATIVE_REGISTRY_URL}", split)
        self.assertIn("${UCLOUD_STORAGE_NATIVE_REPOSITORY}", split)
        self.assertIn("-m ucloud_sandboxes.memory_filesystem", split)
        self.assertIn("ExecStartPre=/usr/bin/env PYTHONPATH=", split)
        self.assertNotIn("ExecStartPre=/usr/bin/env PYTHONPATH=", legacy)
        self.assertIn("--runtime-root ${UCLOUD_STORAGE_NATIVE_MOUNT_ROOT}/.runtime", split)
        self.assertIn("--runtime-root ${UCLOUD_STORAGE_NATIVE_ROOT}/runtime", legacy)
        # Formatting is restricted to a newly-created owned image by the
        # dedicated provisioner, never inline shell device commands.
        self.assertEqual(split.count("mkfs"), legacy.count("mkfs"))
        syntax = subprocess.run(["bash", "-n"], input=split, text=True, capture_output=True)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        with self.assertRaises(ValueError):
            render_vm_init_script(options(role="builder", direct_split_memory_backing=True))

    def test_cli_forwards_only_explicit_split_settings(self):
        args = cli.build_parser().parse_args([
            "serve-direct-node-agent", "--deployment-id", "test", "--job-id", "job", "--state-root", "/tmp/state",
            "--image-file", "/tmp/images", "--volume-mount-root", "/tmp/mounts",
            "--storage-native-socket", "/tmp/native.sock", "--runsc", "/tmp/runsc",
            "--runsc-commit", "a" * 40, "--node-control-bearer-token-file", "/tmp/token",
            "--split-memory-backing", "--memory-backing-hard-capacity-bytes", "123456",
            "--application-memory-root", "/run/test-memory",
            "--reflink-memory-restore",
            "--checkpoint-registry-url", "http://registry:5000",
            "--checkpoint-registry-repository", "snapshots/memory-checkpoints",
        ])
        server = Mock(server_address=("127.0.0.1", 12345))
        service = Mock()
        with patch("ucloud_sandboxes.direct_runtime.build_direct_runtime_service", return_value=service) as build, \
             patch("ucloud_sandboxes.node_agent.build_direct_node_agent_server", return_value=server), \
             patch.object(cli, "read_required_token_file", return_value="secret"), \
             patch.object(cli, "telemetry_from_args", return_value=Mock()), \
             redirect_stdout(StringIO()):
            self.assertEqual(cli.cmd_serve_direct_node_agent(args), 0)
        self.assertTrue(build.call_args.kwargs["split_memory_backing"])
        self.assertTrue(build.call_args.kwargs["reflink_memory_restore"])
        self.assertEqual(build.call_args.kwargs["application_memory_root"], Path("/run/test-memory"))
        self.assertEqual(build.call_args.kwargs["memory_backing_hard_capacity_bytes"], 123456)
        self.assertEqual(build.call_args.kwargs["checkpoint_registry_url"], "http://registry:5000")
        self.assertEqual(build.call_args.kwargs["checkpoint_registry_repository"], "snapshots/memory-checkpoints")

    def test_imported_split_workspace_discards_only_inactive_overlay_scratch(self):
        from tests.test_image_rootfs import Overlay2Runner, image_store
        from ucloud_sandboxes.image_rootfs import OverlayRootfsManager
        from ucloud_sandboxes.checkpoint_components import MemoryBackingRef
        with TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            runner = Overlay2Runner(root / "docker")
            workspace = root / "quota" / "workspace-sandbox.sandbox-1"
            (workspace / "upper").mkdir(parents=True)
            (workspace / "work").mkdir()
            (workspace / "work" / "stale").write_text("source scratch")
            (workspace / "upper" / "data").write_text("keep")
            store = image_store(root, runner)
            manager = OverlayRootfsManager(store, writable_root=root / "quota",
                bundle_root=root / "bundles", runner=runner, require_precreated_writable=True)
            memory = MemoryBackingRef("sandbox.sandbox-1", 1024 ** 3)
            with manager.resolve("example/image:latest") as image:
                lease = manager.prepare(sandbox_id="sandbox", sandbox_generation=1, image=image,
                    config_template={"root": {}, "annotations": {
                        "dev.gvisor.internal.application-memory-directory": memory.allocation_id}},
                    imported_parked=True, workspace_directory=workspace.name, memory=memory)
            self.assertFalse((workspace / "work" / "stale").exists())
            live = workspace / "work" / "live"
            live.write_text("active scratch")
            manager.resume_sandbox(lease.sandbox)
            self.assertTrue(live.exists(), "idempotent resume must not remove live overlay work")
            manager.park_sandbox(lease.sandbox)
            manager.resume_sandbox(lease.sandbox)
            self.assertFalse(live.exists(), "a remount must discard captured stale work")
            self.assertEqual((workspace / "upper" / "data").read_text(), "keep")
            manager.release(lease)

    def test_heartbeat_adds_reserved_bytes_once_and_never_adds_capacity(self):
        with TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            fixture = direct_fixtures.DirectProvisionerTests()
            provisioner, _, storage, _, warden = fixture.make(root)
            service = DirectSandboxService(provisioner, process_runner=direct_fixtures.FakeProcessRunner())
            mib = 1024 ** 2
            native_metrics = {"hard_capacity_bytes": 100 * mib, "hard_reserved_bytes": 20 * mib}
            storage.get_metrics = lambda: native_metrics
            provisioner.checkpoint_store = Mock()
            memory_reserved = [30 * mib]
            overlap_reserved = [2 * mib + 1]
            provisioner.registry.reflink_overlap_bytes = lambda: overlap_reserved[0]
            warden.memory_backing = SimpleNamespace(
                metrics=lambda: {"memory_backing_hard_reserved_bytes": memory_reserved[0]},
                hard_capacity_bytes=100 * mib,
            )
            warden.config.reflink_memory_restore = True
            server = direct_fixtures.build_direct_node_agent_server(
                "127.0.0.1", 0, service=service, image_file=root / "images",
                job_id="job", node_id="node", total_resources=ResourceQuantity(),
                runtime_metrics_provider=lambda: NodeRuntimeMetrics(collected_at=utc_now()),
            )
            try:
                self.assertIn("sandbox-checkpoint-v3", server.RequestHandlerClass.capabilities)
                self.assertIn("sandbox-memory-reflink-restore-v1", server.RequestHandlerClass.capabilities)
                first = server.RequestHandlerClass.runtime_metrics_provider()
                memory_reserved[0] = 40 * mib
                overlap_reserved[0] = 0
                second = server.RequestHandlerClass.runtime_metrics_provider()
                warden.memory_backing.metrics = Mock(side_effect=sqlite3.OperationalError("locked"))
                unavailable = server.RequestHandlerClass.runtime_metrics_provider()
            finally:
                server.server_close()
            self.assertEqual(first.storage_hard_reserved_mb, 53)
            self.assertEqual(second.storage_hard_reserved_mb, 60)
            self.assertEqual(second.storage_hard_capacity_mb, 100)
            self.assertEqual(native_metrics["hard_reserved_bytes"], 20 * mib)
            # Do not publish a partial/native-only reservation as the aggregate.
            self.assertEqual(unavailable.storage_hard_capacity_mb, 0)
            self.assertEqual(unavailable.storage_hard_reserved_mb, 0)
