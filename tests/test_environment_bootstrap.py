import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.environment_producer_key import provision
from tests import test_vm_init as vm_fixtures
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.environment_config import EnvironmentDeploymentConfig, environment_publisher_from_args
from ucloud_sandboxes.vm_init import render_vm_init_script

TEST_TIER = "contract"


class EnvironmentBootstrapTests(unittest.TestCase):
    def test_configured_publisher_isolates_preparation_and_keeps_signing_key_in_parent(self):
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
        from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder

        self.assertIsNone(environment_publisher_from_args(SimpleNamespace()))
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            key = provision(root / "key")
            args = SimpleNamespace(environment_registry_url="http://registry:5000",
                environment_registry_repository="environments",
                environment_trusted_keys=Path(key["public_trust_file"]),
                environment_signing_key=Path(key["private_key_file"]),
                environment_allow_path=["*"], image_file=root / "images.json", docker_binary="docker")
            with patch.object(FreshEnvironmentBuilder, "publish_image", autospec=True,
                              return_value="published-in-parent") as publish:
                publisher = environment_publisher_from_args(args)
                self.assertEqual(publisher(SimpleNamespace(tag="registry:5000/owned/image:latest")),
                                 "published-in-parent")
            builder, image = publish.call_args.args
            self.assertTrue(builder.preparation_subprocess)
            self.assertEqual(image, "registry:5000/owned/image:latest")
            self.assertEqual(publish.call_args.kwargs, {"allowlist": ("*",)})
            self.assertEqual(builder.work_root, root / "environment-build/scratch")
            # A real provisioned private key remains usable by the parent
            # publisher, while the child receives only source preparation data.
            public = Ed25519PublicKey.from_public_bytes(next(iter(builder.registry.trusted_keys.values())))
            challenge = b"parent-retains-environment-signing-authority"
            public.verify(builder.signing_key.sign(challenge), challenge)

    def test_node_advertises_actual_runtime_restore_identity_and_selected_adapter(self):
        from tests import test_direct_provisioner as fixtures
        from ucloud_sandboxes.capabilities import HOST_EROFS_CAPABILITY, RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX
        from ucloud_sandboxes.direct_service import DirectSandboxService
        from ucloud_sandboxes.environment_manifest import HOST_EROFS_ABI
        from ucloud_sandboxes.models import ResourceQuantity
        with TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            provisioner, _, _, _, warden = fixtures.DirectProvisionerTests().make(root)
            provisioner.overlays.image_store.backend_abi = HOST_EROFS_ABI
            provisioner.overlays.image_store.io_metrics = lambda: {"hits": 3}
            service = DirectSandboxService(provisioner, process_runner=fixtures.FakeProcessRunner())
            server = fixtures.build_direct_node_agent_server("127.0.0.1", 0, service=service,
                image_file=root / "images", job_id="job", node_id="node", total_resources=ResourceQuantity())
            try:
                caps = server.RequestHandlerClass.capabilities
                self.assertIn(HOST_EROFS_CAPABILITY, caps)
                self.assertIn(RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX + warden.config.runtime_fingerprint.node_compatibility_sha256, caps)
                metrics = server.RequestHandlerClass.runtime_metrics_provider
                self.assertEqual(metrics().environment_io, {"hits": 3})
                # Read before, and independent of, the storage daemon's metrics.
                with patch.object(service.warden.storage, "get_metrics", side_effect=OSError("storage down")):
                    self.assertEqual(metrics().environment_io, {"hits": 3})
            finally:
                server.server_close()

    def test_config_stays_disabled_until_explicitly_selected_and_roundtrips(self):
        config = DeploymentConfig.default()
        self.assertNotIn("immutable_environments", config.to_dict())
        self.assertIsNone(DeploymentConfig.from_dict(config.to_dict()).immutable_environments)
        raw = config.to_dict()
        raw["immutable_environments"] = {"trusted_keys_file": "/etc/producers.json", "worker_enabled": True}
        parsed = DeploymentConfig.from_dict(raw)
        self.assertTrue(parsed.immutable_environments.worker_enabled)
        self.assertFalse(parsed.immutable_environments.builder_enabled)
        self.assertTrue(parsed.immutable_environments.prefetch_enabled)
        self.assertEqual(DeploymentConfig.from_dict(parsed.to_dict()), parsed)
        raw["immutable_environments"]["prefetch_enabled"] = False
        self.assertFalse(DeploymentConfig.from_dict(raw).immutable_environments.prefetch_enabled)
        for change in ({"worker_enabled": 1}, {"builder_enabled": True}, {"allow_paths": ["../runtime"]},
                       {"prefetch_enabled": "false"}, {"prefetch_enabled": 0}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                EnvironmentDeploymentConfig.from_dict({"trusted_keys_file": "/etc/producers.json", **change})

    def test_layout_two_publication_is_an_explicit_builder_switch(self):
        from ucloud_sandboxes.cli import build_parser
        from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder
        base = {"trusted_keys_file": "/etc/producers.json"}
        self.assertFalse(EnvironmentDeploymentConfig.from_dict(base).preserve_mtimes)
        raw = DeploymentConfig.default().to_dict()
        raw["immutable_environments"] = base | {"preserve_mtimes": True}
        parsed = DeploymentConfig.from_dict(raw)
        self.assertTrue(parsed.immutable_environments.preserve_mtimes)
        self.assertEqual(DeploymentConfig.from_dict(parsed.to_dict()), parsed)
        # Older releases reject the field; it is rendered only once turned on.
        raw["immutable_environments"] = base | {"preserve_mtimes": False}
        rendered = DeploymentConfig.from_dict(raw).to_dict()
        self.assertNotIn("preserve_mtimes", rendered["immutable_environments"])
        self.assertTrue(parsed.to_dict()["immutable_environments"]["preserve_mtimes"])
        self.assertFalse(DeploymentConfig.from_dict(rendered).immutable_environments.preserve_mtimes)
        for value in (1, "true", None):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "preserve_mtimes must be boolean"):
                EnvironmentDeploymentConfig.from_dict(base | {"preserve_mtimes": value})
        args = build_parser().parse_args(["serve-builder-agent", "--deployment-id", "d", "--state-file", "s",
                                          "--image-file", "i", "--node-control-bearer-token-file", "t",
                                          "--environment-preserve-mtimes"])
        self.assertTrue(args.environment_preserve_mtimes)
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            key = provision(root / "key")
            common = {"environment_registry_url": "http://registry:5000", "environment_registry_repository": "environments",
                      "environment_trusted_keys": Path(key["public_trust_file"]),
                      "environment_signing_key": Path(key["private_key_file"]), "environment_allow_path": ["*"],
                      "image_file": root / "images.json", "docker_binary": "docker"}
            for flags, preserve in (({}, False), ({"environment_preserve_mtimes": True}, True)):
                with self.subTest(flags=flags), patch.object(FreshEnvironmentBuilder, "publish_image",
                                                             autospec=True) as publish:
                    environment_publisher_from_args(SimpleNamespace(**common, **flags))(SimpleNamespace(tag="r/i:t"))
                    self.assertEqual(publish.call_args.args[0].preserve_mtimes, preserve)
            with self.assertRaisesRegex(ValueError, "requires registry trust"):
                environment_publisher_from_args(SimpleNamespace(environment_preserve_mtimes=True))
            public = Path(key["public_trust_file"]).read_text()
            private = Path(key["private_key_file"]).read_text()
            common = {"environment_registry_url": "http://registry:5000",
                      "environment_repository": "environments", "environment_trusted_keys_json": public}
            for preserve in (False, True):
                builder = render_vm_init_script(vm_fixtures.VmInitTests._options(role="builder", **common,
                    environment_signing_key_pem=private, environment_allow_paths=("*",),
                    environment_preserve_mtimes=preserve))
                self.assertEqual("--environment-preserve-mtimes" in builder, preserve)
                self.assertEqual("for UCLOUD_MKFS_OPTION in --mkfs-time --MZ;" in builder, preserve)
                syntax = subprocess.run(["bash", "-n"], input=builder, text=True, capture_output=True)
                self.assertEqual(syntax.returncode, 0, syntax.stderr)
            with self.assertRaisesRegex(ValueError, "only builders"):
                render_vm_init_script(vm_fixtures.VmInitTests._options(**common, environment_preserve_mtimes=True))
            with self.assertRaisesRegex(ValueError, "requires registry URL"):
                render_vm_init_script(vm_fixtures.VmInitTests._options(environment_preserve_mtimes=True))

    def test_layout_two_builder_bootstrap_reads_the_whole_mkfs_usage(self):
        from ucloud_sandboxes.environment_bootstrap import settings
        options = SimpleNamespace(role="builder", environment_registry_url="http://registry:5000",
                                  environment_repository="environments", environment_trusted_keys_json="{}",
                                  environment_signing_key_pem="key", environment_allow_paths=("*",),
                                  environment_preserve_mtimes=True)
        check = next(line for line in settings(options)[1].splitlines() if "--mkfs-time" in line)
        filler = r"head -c 100000 /dev/zero | tr '\0' x; "
        with TemporaryDirectory() as temporary:
            tool = Path(temporary) / "mkfs.erofs"
            # erofs-utils writes its long usage in several blocks; a reader that
            # exits at the first match must not fail the bootstrap. Layout 2
            # needs both options, in any order: 1.9 has both, 1.8 lacks --MZ,
            # and 1.4 has neither and exits 1 after its usage.
            for usage, missing in (
                    (r"printf '    --mkfs-time  build time only\n'; sleep 0.2; " + filler
                     + r"printf ' --MZ[=<0|[id]>]  metadata zone\n'; " + filler, None),
                    (r"printf ' --MZ[=<0|[id]>]\n'; " + filler + r"printf '    --mkfs-time\n'", None),
                    (r"printf '    --mkfs-time  build time only\n'; " + filler, "--MZ"),
                    (r"printf ' -T#  fixed UNIX timestamp\n' >&2; exit 1", "--mkfs-time")):
                with self.subTest(usage=usage):
                    tool.write_text("#!/bin/sh\n" + usage + "\n")
                    tool.chmod(0o755)
                    result = subprocess.run(["bash", "-c", "set -euo pipefail\n" + check], capture_output=True,
                                            text=True, env={"PATH": temporary + ":/usr/bin:/bin"})
                    self.assertEqual(result.returncode, 0 if missing is None else 1, result.stderr)
                    self.assertEqual(result.stderr, "" if missing is None else
                                     f"layout-2 publication requires erofs-utils 1.9+ (mkfs.erofs {missing})\n")

    def test_deployment_off_switch_reaches_the_worker_backend_command(self):
        from dataclasses import replace
        from tests.test_policy import node
        from ucloud_sandboxes import cli
        with TemporaryDirectory() as temporary:
            trust = provision(Path(temporary) / "key")["public_trust_file"]
            base = DeploymentConfig.default(scope_id="test")
            for enabled in (True, False):
                selected = EnvironmentDeploymentConfig.from_dict(
                    {"trusted_keys_file": trust, "worker_enabled": True, "prefetch_enabled": enabled})
                with self.subTest(enabled=enabled), patch.object(cli, "read_bearer_token_source", return_value="t"):
                    options = cli.vm_init_options_for_job(
                        replace(base, immutable_environments=selected), node("worker").job, "sandbox",
                        package_spec="/tmp/package.tar.gz", package_sha256="a" * 64)
                    self.assertIs(options.environment_prefetch_enabled, enabled)
                    self.assertEqual("--disable-prefetch" in render_vm_init_script(options), not enabled)

    def test_worker_and_builder_lifetimes_and_key_separation(self):
        with TemporaryDirectory() as temporary:
            key = provision(Path(temporary) / "key")
            public = Path(key["public_trust_file"]).read_text()
            private = Path(key["private_key_file"]).read_text()
            common = {"environment_registry_url": "http://registry:5000",
                      "environment_repository": "environments", "environment_trusted_keys_json": public}
            worker = render_vm_init_script(vm_fixtures.VmInitTests._options(**common))
            self.assertIn("serve-environment-io", worker)
            self.assertIn("systemctl start ucloud-environment-io.service", worker)
            self.assertNotIn("systemctl restart ucloud-environment-io.service", worker)
            self.assertNotIn("PartOf=", worker)
            self.assertIn("PrivateMounts=no", worker)
            self.assertIn("require a fresh worker", worker)
            self.assertNotIn("producer.pem", worker)
            self.assertNotIn("--disable-prefetch", worker)
            disabled = render_vm_init_script(vm_fixtures.VmInitTests._options(**common, environment_prefetch_enabled=False))
            self.assertIn(" --cache-bytes 1073741824 --disable-prefetch --environment-registry-url", disabled)
            builder = render_vm_init_script(vm_fixtures.VmInitTests._options(role="builder", **common,
                environment_signing_key_pem=private, environment_allow_paths=("bin", "etc")))
            self.assertIn("--environment-signing-key", builder)
            self.assertIn("--environment-allow-path bin", builder)
            self.assertNotIn("serve-environment-io", builder)
            self.assertIn("command -v mkfs.erofs", builder)
            builder_unit = builder.split("Description=UCloud sandbox node agent", 1)[1].split("NODE_SERVICE", 1)[0]
            self.assertIn("User=root", builder_unit)
            self.assertIn("chown root:root /etc/ucloud-sandboxes/environment/producer.pem", builder)
            for script in (worker, builder):
                syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True)
                self.assertEqual(syntax.returncode, 0, syntax.stderr)
            with self.assertRaisesRegex(ValueError, "never receive"):
                render_vm_init_script(vm_fixtures.VmInitTests._options(**common, environment_signing_key_pem=private))
            with self.assertRaisesRegex(ValueError, "headroom"):
                render_vm_init_script(vm_fixtures.VmInitTests._options(**common, environment_cache_bytes=100 * 1024 ** 3))
            # A truthy string must not render the default (prefetching) service.
            with self.assertRaisesRegex(ValueError, "prefetch_enabled must be boolean"):
                render_vm_init_script(vm_fixtures.VmInitTests._options(**common, environment_prefetch_enabled="false"))
            altered = json.loads(public)
            altered[next(iter(altered))] = "bad"
            with self.assertRaises(ValueError):
                render_vm_init_script(vm_fixtures.VmInitTests._options(**{**common, "environment_trusted_keys_json": json.dumps(altered)}))


if __name__ == "__main__":
    unittest.main()
