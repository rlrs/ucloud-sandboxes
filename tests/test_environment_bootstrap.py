import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest

from scripts.environment_producer_key import provision
from tests import test_vm_init as vm_fixtures
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.environment_config import EnvironmentDeploymentConfig
from ucloud_sandboxes.vm_init import render_vm_init_script


class EnvironmentBootstrapTests(unittest.TestCase):
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
            service = DirectSandboxService(provisioner, process_runner=fixtures.FakeProcessRunner())
            server = fixtures.build_direct_node_agent_server("127.0.0.1", 0, service=service,
                image_file=root / "images", job_id="job", node_id="node", total_resources=ResourceQuantity())
            try:
                caps = server.RequestHandlerClass.capabilities
                self.assertIn(HOST_EROFS_CAPABILITY, caps)
                self.assertIn(RUNTIME_COMPATIBILITY_CAPABILITY_PREFIX + warden.config.runtime_fingerprint.node_compatibility_sha256, caps)
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
        self.assertEqual(DeploymentConfig.from_dict(parsed.to_dict()), parsed)
        for change in ({"worker_enabled": 1}, {"builder_enabled": True}, {"allow_paths": ["../runtime"]}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                EnvironmentDeploymentConfig.from_dict({"trusted_keys_file": "/etc/producers.json", **change})

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
            altered = json.loads(public)
            altered[next(iter(altered))] = "bad"
            with self.assertRaises(ValueError):
                render_vm_init_script(vm_fixtures.VmInitTests._options(**{**common, "environment_trusted_keys_json": json.dumps(altered)}))


if __name__ == "__main__":
    unittest.main()
