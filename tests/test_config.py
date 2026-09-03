import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes.config import DeploymentConfig


class ConfigTests(unittest.TestCase):
    @staticmethod
    def _raw() -> dict[str, object]:
        return DeploymentConfig.default(scope_id="project-1").to_dict()

    def test_exact_config_round_trip_and_derived_authority(self) -> None:
        raw = self._raw()
        sandbox = raw["sandbox"]
        builder = raw["builder"]
        assert isinstance(sandbox, dict)
        assert isinstance(builder, dict)
        sandbox["disk_gb"] = 1000
        sandbox["docker_quota_image_gb"] = 200
        sandbox["swap_gb"] = 50
        sandbox["storage_native_cache_gb"] = 25
        sandbox["direct_disk_headroom_mb"] = 10 * 1024
        builder["scale_down_idle_seconds"] = 321
        raw["gateway_heartbeat_ttl_seconds"] = 45

        config = DeploymentConfig.from_dict(raw)

        self.assertEqual(config.policy.heartbeat_ttl_seconds, 45)
        self.assertEqual(config.policy.builder_scale_down_idle_seconds, 321)
        self.assertEqual(config.policy.default_node_resources.vcpu, 32)
        self.assertEqual(
            config.policy.default_node_resources.disk_mb,
            1000 * 1024 - 200 * 1024 - 50 * 1024 - 25 * 1024 - 10 * 1024,
        )
        self.assertNotIn("heartbeat_ttl_seconds", config.to_dict()["policy"])
        self.assertNotIn("default_node_resources", config.to_dict()["policy"])
        self.assertEqual(DeploymentConfig.from_dict(config.to_dict()), config)

    def test_external_provider_owns_exact_provider_payload(self) -> None:
        raw = self._raw()
        raw["provider"] = {
            "kind": "examplecloud",
            "scope_id": "tenant-a",
            "region": "north-1",
            "machine_profile": "sandbox-large",
        }

        config = DeploymentConfig.from_dict(raw)

        self.assertEqual(config.provider.kind, "examplecloud")
        self.assertEqual(config.provider.scope_id, "tenant-a")
        self.assertEqual(
            config.provider.settings,
            {"region": "north-1", "machine_profile": "sandbox-large"},
        )

    def test_file_reader_has_no_old_or_partial_schema(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "deployment.json"
            for payload in (
                {"provider": {"kind": "ucloud", "scope_id": "project-1"}},
                {"schema": 0},
                {"schema": 1, "state_dir": "/tmp/old"},
            ):
                with self.subTest(payload=payload):
                    path.write_text(json.dumps(payload), encoding="utf-8")
                    with self.assertRaises(ValueError):
                        DeploymentConfig.from_file(path)

    def test_rejects_missing_extra_and_runtime_policy_aliases(self) -> None:
        cases: list[tuple[str, dict[str, object]]] = []
        missing = self._raw()
        missing.pop("builder")
        cases.append(("missing", missing))
        extra = self._raw()
        extra["state_dir"] = "/tmp/old"
        cases.append(("extra", extra))
        policy_alias = self._raw()
        policy = policy_alias["policy"]
        assert isinstance(policy, dict)
        policy["heartbeat_ttl_seconds"] = 120
        cases.append(("policy alias", policy_alias))
        provider_alias = self._raw()
        provider = provider_alias["provider"]
        assert isinstance(provider, dict)
        provider["session_file"] = "/tmp/session.json"
        cases.append(("provider alias", provider_alias))

        for label, raw in cases:
            with self.subTest(label=label), self.assertRaises(ValueError):
                DeploymentConfig.from_dict(raw)

    def test_rejects_wrong_types_and_invalid_ranges(self) -> None:
        cases: list[tuple[str, tuple[str, ...], object]] = [
            ("bool integer", ("gateway_port",), True),
            ("string number", ("registry_retention_days",), "30"),
            ("nan", ("autoscaler_interval_seconds",), float("nan")),
            ("relative root", ("data_root",), "state"),
            (
                "relative registry root",
                ("registry_store", "data_root"),
                "registry",
            ),
            (
                "relative registry mount",
                ("registry_store", "mount_point"),
                "registry",
            ),
            ("colliding port", ("relay_port",), 8090),
            ("wrong tuple", ("sandbox", "direct_network_allow_tcp"), "10.0.0.1:1"),
            ("low disk", ("sandbox", "disk_gb"), 1),
            ("bad builder disk", ("builder", "disk_gb"), 200),
            ("bool policy int", ("policy", "max_nodes"), True),
            ("string policy bool", ("policy", "live_pressure_enabled"), "true"),
        ]
        for label, path, value in cases:
            raw = self._raw()
            target: dict[str, object] = raw
            for part in path[:-1]:
                nested = target[part]
                assert isinstance(nested, dict)
                target = nested
            target[path[-1]] = value
            with self.subTest(label=label), self.assertRaises(ValueError):
                DeploymentConfig.from_dict(raw)

    def test_rejects_device_pool_larger_than_ublk_capacity(self) -> None:
        raw = self._raw()
        sandbox = raw["sandbox"]
        assert isinstance(sandbox, dict)
        sandbox["storage_native_pool_high_watermark"] = 17
        sandbox["storage_native_max_ublk_devices"] = 16

        with self.assertRaisesRegex(ValueError, "cannot exceed"):
            DeploymentConfig.from_dict(raw)

    def test_sqlite_and_registry_roots_are_independent(self) -> None:
        raw = self._raw()
        raw["data_root"] = "/srv/ucloud/state"
        registry_store = raw["registry_store"]
        assert isinstance(registry_store, dict)
        registry_store["mount_point"] = "/mnt/registry"
        registry_store["data_root"] = "/mnt/registry/docker-registry"

        config = DeploymentConfig.from_dict(raw)

        self.assertEqual(
            config.control_state_file(), Path("/srv/ucloud/state/control-state.sqlite")
        )
        self.assertEqual(config.routing_file(), Path("/srv/ucloud/state/routes.sqlite"))
        self.assertEqual(config.image_file(), Path("/srv/ucloud/state/images.sqlite"))
        self.assertEqual(
            config.registry_usage_file(),
            Path("/srv/ucloud/state/registry-usage.sqlite"),
        )
        self.assertEqual(
            config.metrics_path(), Path("/srv/ucloud/state/metrics.sqlite")
        )
        self.assertEqual(
            config.registry_data_dir(),
            Path("/mnt/registry/docker-registry"),
        )

    def test_schema_one_is_rejected_without_legacy_migration(self) -> None:
        raw = self._raw()
        raw["schema"] = 1
        raw["data_root"] = "/srv/ucloud/state"
        raw.pop("registry_store")
        raw.pop("snapshot_store")
        raw["autoscaler_max_storage_native_migrations_per_cycle"] = raw.pop(
            "autoscaler_max_storage_native_detaches_per_cycle"
        )

        with self.assertRaisesRegex(ValueError, "unsupported deployment config schema"):
            DeploymentConfig.from_dict(raw)

    def test_schema_three_is_rejected_without_legacy_migration(self) -> None:
        raw = self._raw()
        raw["schema"] = 3
        registry_store = raw.pop("registry_store")
        assert isinstance(registry_store, dict)
        raw["registry_mount_point"] = registry_store["mount_point"]
        raw["registry_data_root"] = registry_store["data_root"]

        with self.assertRaisesRegex(ValueError, "unsupported deployment config schema"):
            DeploymentConfig.from_dict(raw)

    def test_registry_data_must_be_inside_its_fail_closed_mount(self) -> None:
        raw = self._raw()
        registry_store = raw["registry_store"]
        assert isinstance(registry_store, dict)
        registry_store["mount_point"] = "/mnt/registry"
        registry_store["data_root"] = "/var/lib/registry"

        with self.assertRaisesRegex(ValueError, "inside registry_store.mount_point"):
            DeploymentConfig.from_dict(raw)

    def test_s3_registry_store_has_no_filesystem_dependency(self) -> None:
        raw = self._raw()
        raw["registry_store"] = {
            "kind": "s3",
            "mount_point": "",
            "data_root": "",
            "endpoint": "https://images.hel1.your-objectstorage.com/",
            "bucket": "images",
            "region": "hel1",
            "prefix": "/production/oci/",
            "access_key_id_env": "REGISTRY_ACCESS_KEY",
            "secret_access_key_env": "REGISTRY_SECRET_KEY",
            "force_path_style": False,
        }

        config = DeploymentConfig.from_dict(raw)

        self.assertEqual(config.registry_store.kind, "s3")
        self.assertEqual(
            config.registry_store.endpoint,
            "https://hel1.your-objectstorage.com",
        )
        self.assertEqual(config.registry_store.prefix, "production/oci")
        with self.assertRaisesRegex(ValueError, "no local registry data directory"):
            config.registry_data_dir()

    def test_s3_snapshot_store_keeps_only_environment_names_in_state(self) -> None:
        raw = self._raw()
        raw["snapshot_store"] = {
            "kind": "s3",
            "endpoint": "https://fsn1.your-objectstorage.com",
            "bucket": "sandbox-snapshots",
            "region": "fsn1",
            "prefix": "/production/",
            "access_key_id_env": "SNAPSHOT_ACCESS_KEY",
            "secret_access_key_env": "SNAPSHOT_SECRET_KEY",
            "security_token_env": "SNAPSHOT_SECURITY_TOKEN",
        }

        secret = "secret-access-key-value"
        with patch.dict(os.environ, {"SNAPSHOT_SECRET_KEY": secret}):
            config = DeploymentConfig.from_dict(raw)

        self.assertEqual(config.snapshot_store.kind, "s3")
        self.assertEqual(config.snapshot_store.prefix, "production")
        encoded = json.dumps(config.to_dict())
        self.assertIn("SNAPSHOT_SECRET_KEY", encoded)
        self.assertNotIn(secret, encoded)

    def test_s3_snapshot_store_rejects_literal_credentials(self) -> None:
        raw = self._raw()
        snapshot_store = raw["snapshot_store"]
        assert isinstance(snapshot_store, dict)
        snapshot_store["secret_access_key"] = "secret-access-key-value"

        with self.assertRaisesRegex(ValueError, "secret_access_key"):
            DeploymentConfig.from_dict(raw)

    def test_s3_snapshot_store_accepts_hetzner_bucket_url_as_endpoint(self) -> None:
        raw = self._raw()
        raw["snapshot_store"] = {
            "kind": "s3",
            "endpoint": "https://sandboxes.hel1.your-objectstorage.com/",
            "bucket": "sandboxes",
            "region": "hel1",
            "prefix": "production",
            "access_key_id_env": "SNAPSHOT_ACCESS_KEY",
            "secret_access_key_env": "SNAPSHOT_SECRET_KEY",
            "security_token_env": "SNAPSHOT_SECURITY_TOKEN",
        }

        config = DeploymentConfig.from_dict(raw)

        self.assertEqual(
            config.snapshot_store.endpoint,
            "https://hel1.your-objectstorage.com",
        )

    def test_s3_snapshot_store_rejects_non_origin_endpoint(self) -> None:
        raw = self._raw()
        snapshot_store = raw["snapshot_store"]
        assert isinstance(snapshot_store, dict)
        snapshot_store.update(
            {
                "kind": "s3",
                "endpoint": "https://hel1.your-objectstorage.com/path",
                "bucket": "sandboxes",
                "region": "hel1",
            }
        )

        with self.assertRaisesRegex(ValueError, "origin"):
            DeploymentConfig.from_dict(raw)


if __name__ == "__main__":
    unittest.main()
