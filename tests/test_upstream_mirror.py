"""C2.15: the opt-in pull-through mirror for upstream registries."""
from __future__ import annotations

from dataclasses import replace
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.deploy import REGISTRY_STORAGE_SYSTEMD_UNITS, SYSTEMD_UNIT_NAMES, packaged_systemd_units
from ucloud_sandboxes.image_import import import_build_context
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.systemd import (
    read_mirror_credentials, reconcile_gateway_services, run_upstream_mirror, trim_upstream_mirrors,
    upstream_mirror_command, upstream_mirror_environment,
)
import ucloud_sandboxes.vm_init as vm_init

ROOT = Path(__file__).resolve().parents[1]
with patch.object(sys, "path", [str(ROOT / "scripts"), *sys.path]):
    import prepare_image_pool as pool

MIRROR = {"listen_address": "10.42.0.2", "storage_root": "/work/data/upstream-mirror",
          "upstreams": [{"registry": "docker.io", "port": 5010, "credentials_file": "/etc/hub.env"},
                        {"registry": "ghcr.io", "port": 5011}]}


def mirrored(store=None, **changes) -> DeploymentConfig:
    raw = DeploymentConfig.default("project").to_dict()
    raw["registry_store"].update(store or {})
    raw["builder"]["buildx_cache_ref"] = "sandbox-gateway-production:5000/ucloud-build-cache:shared"
    raw["upstream_mirror"] = {**MIRROR, **changes}
    return DeploymentConfig.from_dict(raw)


class UpstreamMirrorConfigTests(unittest.TestCase):
    def test_default_off_round_trips_and_validates_strictly(self):
        self.assertNotIn("upstream_mirror", DeploymentConfig.default().to_dict())
        config = mirrored()
        self.assertEqual((config.upstream_mirror.ttl_hours, config.upstream_mirror.max_bytes), (168, 256 * 1024**3))
        self.assertEqual(DeploymentConfig.from_dict(config.to_dict()), config)
        self.assertEqual(config.upstream_mirror_authorities(), {"docker.io": "10.42.0.2:5010", "ghcr.io": "10.42.0.2:5011"})
        self.assertEqual(config.upstream_mirror.upstreams[0].remote_url, "https://registry-1.docker.io")
        wildcard = mirrored(listen_address="0.0.0.0")
        self.assertEqual(wildcard.upstream_mirror_authorities()["ghcr.io"], "sandbox-gateway-production:5011")
        self.assertEqual(wildcard.upstream_mirror.local_url(wildcard.upstream_mirror.upstreams[1]), "http://127.0.0.1:5011")
        hub = [{"registry": "docker.io", "port": 5010}]
        for changes in ({"listen_address": "127.0.0.1"}, {"listen_address": "77.42.92.27"}, {"proxy": True},
                        {"upstreams": []}, {"upstreams": [{"registry": "registry-1.docker.io", "port": 5010}]},
                        {"upstreams": hub * 2}, {"upstreams": [{"registry": "docker.io", "port": 5000}]},
                        {"storage_root": "/var/lib/mirror"},
                        {"storage_root": "/work/data/ucloud-sandbox-registry/docker-registry/mirror"},
                        {"ttl_hours": 0}, {"max_bytes": 1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                mirrored(**changes)
        raw = mirrored().to_dict()
        raw["builder"]["buildx_cache_ref"] = ""
        with self.assertRaisesRegex(ValueError, "other than docker.io require builder.buildx_cache_ref"):
            DeploymentConfig.from_dict(raw)
        raw["upstream_mirror"]["upstreams"] = hub
        self.assertIsNotNone(DeploymentConfig.from_dict(raw).upstream_mirror)


class UpstreamMirrorServiceTests(unittest.TestCase):
    def test_units_are_packaged_gated_on_the_volume_and_installed(self):
        units = packaged_systemd_units()
        service = units["ucloud-sandbox-upstream-mirror@.service"]
        self.assertIn("ucloud_sandboxes.systemd upstream-mirror --config /etc/ucloud-sandboxes/deployment.json --upstream %i", service)
        self.assertIn("ExecStop=/usr/bin/docker stop ucloud-sandbox-upstream-mirror-%i", service)
        self.assertIn("Restart=always", service)
        self.assertIn("SuccessExitStatus=78\nRestartPreventExitStatus=78", service)
        self.assertIn("upstream-mirror-trim", units["ucloud-sandbox-upstream-mirror-trim.service"])
        self.assertIn("OnCalendar=hourly", units["ucloud-sandbox-upstream-mirror-trim.timer"])
        self.assertIn("ucloud-sandbox-upstream-mirror-trim.timer", SYSTEMD_UNIT_NAMES)
        self.assertIn("ucloud-sandbox-upstream-mirror@.service", REGISTRY_STORAGE_SYSTEMD_UNITS)
        installer = (ROOT / "scripts/install_hetzner_gateway.sh").read_text()
        self.assertIn("ucloud-sandbox-upstream-mirror@.service \\\n  ucloud-sandbox-upstream-mirror-trim.service; do", installer)
        self.assertIn('install -D -m 0600 -o root -g root "$staged_credentials" "$mirror_credentials_file"', installer)

    def test_proxy_environment_keeps_credentials_out_of_arguments(self):
        config = mirrored()
        hub, ghcr = config.upstream_mirror.upstreams
        with TemporaryDirectory() as raw:
            secret = Path(raw) / "hub.env"
            secret.write_text("# Docker Hub\nREGISTRY_PROXY_USERNAME=robot\nREGISTRY_PROXY_PASSWORD=dckr_pat_secret\n")
            secret.chmod(0o600)
            hub = type(hub)("docker.io", 5010, str(secret))
            environment = upstream_mirror_environment(config, hub, environ={"REGISTRY_PROXY_PASSWORD": "stale"})
            self.assertEqual(environment["REGISTRY_PROXY_PASSWORD"], "dckr_pat_secret")
            secret.chmod(0o644)
            with self.assertRaisesRegex(RuntimeError, "0600"):
                read_mirror_credentials(secret)
            secret.write_text("REGISTRY_PROXY_PASSWORD=dckr_pat_secret\nTOKEN=x\n")
            secret.chmod(0o600)
            with self.assertRaises(RuntimeError) as raised:
                read_mirror_credentials(secret)
            self.assertNotIn("dckr_pat_secret", str(raised.exception))
        command = upstream_mirror_command(config, hub)
        self.assertNotIn("dckr_pat_secret", " ".join(command))
        self.assertIn("REGISTRY_PROXY_PASSWORD", command)
        self.assertIn("/work/data/upstream-mirror/docker.io:/var/lib/registry", command)
        self.assertEqual(environment["REGISTRY_HTTP_ADDR"], "10.42.0.2:5010")
        self.assertEqual(environment["REGISTRY_PROXY_REMOTEURL"], "https://registry-1.docker.io")
        self.assertEqual(environment["REGISTRY_PROXY_TTL"], "168h")
        anonymous = upstream_mirror_environment(config, ghcr, environ={"REGISTRY_PROXY_PASSWORD": "stale"})
        self.assertNotIn("REGISTRY_PROXY_PASSWORD", anonymous)
        self.assertNotIn("REGISTRY_PROXY_PASSWORD", upstream_mirror_command(config, ghcr))
        runner = Mock()
        self.assertEqual(run_upstream_mirror(DeploymentConfig.default(), "docker.io", runner=runner), 78)
        runner.assert_not_called()

    def test_reconcile_starts_configured_instances_and_stops_removed_ones(self):
        def reconcile(config):
            commands, health = [], []
            reconcile_gateway_services(config=config, runner=lambda command, *, check, text: commands.append(command),
                                       wait_for=lambda name, url: health.append(url))
            return commands, health
        commands, health = reconcile(mirrored())
        self.assertIn(["systemctl", "stop", "ucloud-sandbox-upstream-mirror@*.service"], commands)
        self.assertLess(commands.index(["systemctl", "stop", "ucloud-sandbox-upstream-mirror@*.service"]),
                        commands.index(["systemctl", "enable", "--now", "ucloud-sandbox-upstream-mirror@docker.io.service"]))
        self.assertIn(["systemctl", "enable", "--now", "ucloud-sandbox-upstream-mirror-trim.timer"], commands)
        self.assertEqual(health[:2], ["http://10.42.0.2:5010/v2/", "http://10.42.0.2:5011/v2/"])
        commands, health = reconcile(DeploymentConfig.default("project"))
        self.assertIn(["systemctl", "disable", "--now", "ucloud-sandbox-upstream-mirror-trim.timer"], commands)
        self.assertFalse([c for c in commands if c[1] == "enable" and "upstream-mirror@" in c[-1]])

    def test_trim_empties_the_largest_caches_until_within_the_bound(self):
        with TemporaryDirectory() as raw:
            config = mirrored({"mount_point": raw, "data_root": raw + "/registry"},
                              storage_root=raw + "/mirror", max_bytes=1024**3)
            for upstream in config.upstream_mirror.upstreams:
                (config.upstream_mirror.storage_dir(upstream) / "docker").mkdir(parents=True)
            sizes = {"docker.io": 1024**3, "ghcr.io": 2 * 1024**3}
            commands = []
            result = trim_upstream_mirrors(config, runner=lambda command, **_: commands.append(command),
                                           measure=lambda path: sizes[path.name])
            self.assertEqual(result["purged"], ["ghcr.io"])
            self.assertEqual(commands, [["systemctl", "stop", "ucloud-sandbox-upstream-mirror@ghcr.io.service"],
                                        ["systemctl", "start", "ucloud-sandbox-upstream-mirror@ghcr.io.service"]])
            self.assertFalse(Path(raw, "mirror/ghcr.io").exists())
            self.assertTrue(Path(raw, "mirror/docker.io/docker").exists())
        self.assertEqual(trim_upstream_mirrors(DeploymentConfig.default())["action"], "none")


class BuilderMirrorTests(unittest.TestCase):
    def options(self, **changes):
        return vm_init.VmInitOptions(
            job_id="job-1", deployment_id="test", heartbeat_url="http://gateway:8090/v1/nodes/heartbeat", role="builder",
            heartbeat_bearer_token_file="/run/h", heartbeat_bearer_token="h", node_control_bearer_token_file="/run/n",
            node_control_bearer_token="n", package_spec="/tmp/node.tar.gz", package_sha256="a" * 64, docker_quota_image_gb=160,
            total_resources=ResourceQuantity(vcpu=8, memory_mb=32768, disk_mb=223 * 1024),
            buildx_cache_ref="10.42.0.2:5000/ucloud-build-cache:shared", buildx_cache_registry_url="http://10.42.0.2:5000",
            docker_insecure_registries=("10.42.0.2:5000",),
            registry_mirrors=tuple(f"{r}={a}" for r, a in mirrored().upstream_mirror_authorities().items()), **changes)

    def test_buildkit_and_docker_route_upstreams_through_the_mirrors(self):
        config = vm_init._buildkit_config(self.options())
        self.assertIn('[registry."docker.io"]\n  mirrors = ["10.42.0.2:5010"]', config)
        self.assertIn('[registry."ghcr.io"]\n  mirrors = ["10.42.0.2:5011"]', config)
        for authority in ("10.42.0.2:5000", "10.42.0.2:5010", "10.42.0.2:5011"):
            self.assertIn(f'[registry."{authority}"]\n  http = true', config)
        script = vm_init.render_vm_init_script(self.options())
        snippet = script.split("""python3 - <<'PY' > "$DOCKER_DAEMON_JSON"\n""", 1)[1].split("\nPY\n", 1)[0]
        environment = {**os.environ, "UCLOUD_DOCKER_DATA_ROOT": "/d", "UCLOUD_DOCKER_QUOTA_IMAGE_GB": "0",
                       "UCLOUD_DOCKER_MAX_CONCURRENT_DOWNLOADS": "3"}
        for name in ("UCLOUD_DOCKER_INSECURE_REGISTRIES_JSON", "UCLOUD_DOCKER_REGISTRY_MIRRORS_JSON"):
            environment[name] = shlex.split(script.split(f"\n{name}=", 1)[1].split("\n", 1)[0])[0]
        daemon = json.loads(subprocess.run([sys.executable, "-c", snippet], env=environment, check=True,
                                           capture_output=True, text=True).stdout)
        self.assertEqual(daemon["registry-mirrors"], ["http://10.42.0.2:5010"])
        self.assertEqual(daemon["insecure-registries"], ["10.42.0.2:5000", "10.42.0.2:5010", "10.42.0.2:5011"])
        plain = vm_init.render_vm_init_script(replace(self.options(), registry_mirrors=()))
        self.assertIn("UCLOUD_DOCKER_REGISTRY_MIRRORS_JSON='[]'", plain)
        with self.assertRaisesRegex(ValueError, "UPSTREAM=HOST:PORT"):
            vm_init.render_vm_init_script(replace(self.options(), registry_mirrors=("docker.io=http://10.42.0.2:5010",)))


class ImportRoutingTests(unittest.TestCase):
    def test_imports_keep_their_identity_and_campaign_pulls_use_the_mirror(self):
        # Request-time imports are builds of FROM <image>; builders route the
        # pull, so the recorded source and import identity never change.
        self.assertIn(b"FROM ghcr.io/org/tool:1\n", gzip.decompress(import_build_context("ghcr.io/org/tool:1")))
        mirrors = pool.deployment_mirrors(mirrored())
        self.assertEqual(pool.upstream_endpoint("docker.io", mirrors), ("http://10.42.0.2:5010", False))
        self.assertEqual(pool.upstream_endpoint("quay.io", mirrors), ("https://quay.io", True))
        self.assertEqual(pool.upstream_endpoint("docker.io"), ("https://registry-1.docker.io", True))
        image_config = b'{"os":"linux","architecture":"amd64","rootfs":{"diff_ids":[]}}'
        manifest = json.dumps({"schemaVersion": 2, "layers": [],
                               "config": {"digest": "sha256:" + hashlib.sha256(image_config).hexdigest()}}).encode()
        urls = []

        def urlopen(request, timeout):
            urls.append((request.full_url, request.headers))
            body = manifest if "/manifests/" in request.full_url else image_config
            response = io.BytesIO(body)
            response.headers = {}
            response.__enter__, response.__exit__ = lambda: response, lambda *a: None
            return response
        with patch.object(pool.request, "urlopen", side_effect=urlopen), \
                patch.object(pool, "public_registry_headers", side_effect=AssertionError("no upstream token")):
            resolved = pool.resolve_source("python:3.11", mirrors)
        self.assertEqual(urls[0][0], "http://10.42.0.2:5010/v2/library/python/manifests/3.11")
        self.assertNotIn("Authorization", urls[0][1])
        self.assertTrue(resolved["reference"].startswith("docker.io/library/python@sha256:"))


if __name__ == "__main__":
    unittest.main()
