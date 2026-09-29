from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import Mock, patch

from tests.test_registry_sweep import FakeDistribution, HOUR
from ucloud_sandboxes import build_cache, cli
from ucloud_sandboxes.build_cache import CACHE_CONFIG_MEDIA_TYPE, CACHE_MANIFEST_MEDIA_TYPE
from ucloud_sandboxes.config import DeploymentConfig
from ucloud_sandboxes.managed_registry import RegistryTag, RegistryUsageStore
from ucloud_sandboxes.registry_sweep import sweep_registry_blobs


CACHE_REPOSITORY = "test/ucloud-build-cache"
IMAGE_REPOSITORY = "test/images"


def digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


class MaintenanceRegistry:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.records = {
            IMAGE_REPOSITORY: {"old-image": digest("image")},
            CACHE_REPOSITORY: {"cache-owned-by-separate-policy": digest("cache")},
        }
        self.age_scanned: list[str] = []
        self.deleted: list[tuple[str, str]] = []

    def catalog(self):
        return list(self.records)

    def tags(self, repository):
        return list(self.records[repository])

    def tag_record(self, repository, tag):
        self.age_scanned.append(repository)
        return RegistryTag(
            repository, tag, self.records[repository][tag],
            created_at="2000-01-01T00:00:00+00:00",
        )

    def delete_manifest(self, repository, manifest_digest):
        self.deleted.append((repository, manifest_digest))


class BuildCacheMaintenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        default = DeploymentConfig.default("project")
        raw = default.to_dict()
        raw["data_root"] = str(self.root / "state")
        raw["registry_store"]["mount_point"] = str(self.root)
        raw["registry_store"]["data_root"] = str(self.root / "registry")
        raw["builder"].update({
            "buildx_cache_ref": f"{default.registry_endpoint_host}:{default.registry_port}/{CACHE_REPOSITORY}:shared",
            "buildx_cache_max_bytes": 2 * 1024**3,
            "buildx_cache_max_entries": 5,
            "buildx_cache_max_age_seconds": 900,
        })
        self.config = DeploymentConfig.from_dict(raw)
        self.registry = MaintenanceRegistry(self.config.registry_url)
        RegistryUsageStore(self.config.registry_usage_file()).touch_images(
            (f"registry:5000/{repository}:{tag}" for repository, tags in self.registry.records.items() for tag in tags),
            when=datetime(2000, 1, 1, tzinfo=timezone.utc),
        )

    def run_maintenance(self, *, execute=True, prefix="", cache_result=None, error=None):
        policy = Mock(spec=["repository", "prune"])
        policy.repository = CACHE_REPOSITORY
        policy.prune.return_value = deepcopy(cache_result or {"deleted_manifests": 0})
        policy.prune.side_effect = error
        with ExitStack() as stack:
            stack.enter_context(patch.object(cli, "RegistryClient", return_value=self.registry))
            stack.enter_context(patch.object(cli, "registry_disk_usage", return_value=None))
            factory = stack.enter_context(patch.object(build_cache, "RegistryBuildCache", return_value=policy))
            result = cli.run_registry_prune(self.config, execute=execute, repository_prefix=prefix)
        return result, policy, factory

    def test_dry_run_excludes_cache_from_image_age_rules_and_passes_bounds(self) -> None:
        result, policy, factory = self.run_maintenance(execute=False)

        self.assertEqual({record["repository"] for record in result["tags"]}, {IMAGE_REPOSITORY})
        self.assertEqual({record["repository"] for record in result["delete"]}, {IMAGE_REPOSITORY})
        self.assertNotIn(CACHE_REPOSITORY, self.registry.age_scanned)
        self.assertEqual(self.registry.deleted, [])
        policy.prune.assert_called_once_with(execute=False)
        factory.assert_called_once_with(
            self.config.builder.buildx_cache_ref,
            registry_url=self.config.registry_url,
            max_bytes=2 * 1024**3,
            max_entries=5,
            max_age_seconds=900,
        )

    def test_execution_revalidation_also_excludes_cache_from_image_age_rules(self) -> None:
        result, policy, _factory = self.run_maintenance()

        self.assertNotIn(CACHE_REPOSITORY, self.registry.age_scanned)
        self.assertGreaterEqual(self.registry.age_scanned.count(IMAGE_REPOSITORY), 2)
        self.assertEqual(self.registry.deleted, [(IMAGE_REPOSITORY, digest("image"))])
        self.assertEqual(result["deleted_manifest_count"], 1)
        policy.prune.assert_called_once_with(execute=True)

    def test_cache_policy_only_runs_for_matching_repository_prefix(self) -> None:
        for prefix, expected in (
            ("", True),
            ("test/", True),
            (CACHE_REPOSITORY, True),
            (IMAGE_REPOSITORY, False),
            ("another-project/", False),
        ):
            with self.subTest(prefix=prefix):
                result, policy, _factory = self.run_maintenance(execute=False, prefix=prefix)
                if expected:
                    policy.prune.assert_called_once_with(execute=False)
                    self.assertIn("build_cache", result)
                else:
                    policy.prune.assert_not_called()
                    self.assertNotIn("build_cache", result)

    def test_partial_cache_success_contributes_to_gc_accounting(self) -> None:
        partial = {"deleted_manifests": 2, "error": "OSError", "incomplete": True}
        result, _policy, _factory = self.run_maintenance(cache_result=partial)
        state = json.loads(self.config.registry_maintenance_state_file().read_text())

        self.assertEqual(result["build_cache"], partial)
        self.assertEqual(result["deleted_manifest_count"], 3)
        self.assertEqual(state["deleted_since_gc"], 3)
        self.assertEqual(state["last_prune_deleted"], 3)
        self.assertEqual(self.registry.deleted, [(IMAGE_REPOSITORY, digest("image"))])

    def test_cache_inventory_failure_does_not_interrupt_image_pruning(self) -> None:
        for error in (OSError("unavailable"), ValueError("malformed inventory"), RuntimeError("deferred")):
            with self.subTest(error=type(error).__name__):
                self.registry.deleted.clear()
                result, _policy, _factory = self.run_maintenance(error=error)
                self.assertEqual(self.registry.deleted, [(IMAGE_REPOSITORY, digest("image"))])
                self.assertEqual(result["deleted_manifest_count"], 1)
                self.assertEqual(result["build_cache"], {
                    "error": type(error).__name__, "deleted_manifests": 0,
                })


class BuildCacheGarbageCollectionTests(unittest.TestCase):
    def test_flat_buildkit_cache_closure_keeps_config_and_layers_until_unreferenced(self) -> None:
        with TemporaryDirectory() as directory:
            now = time.time()
            registry = FakeDistribution(Path(directory), now=now)
            shared = registry.blob(b"layer shared by final image and cache")
            cache_only = registry.blob(b"cache intermediate layer")
            cache_config = registry.blob(b'{"layers":[],"records":[]}')
            image_manifest = registry.manifest(IMAGE_REPOSITORY, layers=[shared])
            document = {
                "schemaVersion": 2,
                "mediaType": CACHE_MANIFEST_MEDIA_TYPE,
                "config": {"mediaType": CACHE_CONFIG_MEDIA_TYPE, "digest": cache_config, "size": 26},
                "layers": [{"digest": value, "size": 1} for value in (shared, cache_only)],
            }
            cache_manifest = registry.blob(json.dumps(document).encode())
            registry.link(registry.tree.repositories / CACHE_REPOSITORY / "_manifests/revisions", cache_manifest)
            for value in (cache_config, shared, cache_only):
                registry.layer_link(CACHE_REPOSITORY, value)

            self.assertEqual(registry.tree.closure([cache_manifest]), {
                cache_manifest, cache_config, shared, cache_only,
            })
            kept = sweep_registry_blobs(registry.root, grace_seconds=2 * HOUR, writers_stopped=True)
            self.assertEqual(kept.deleted_blobs, 0)

            registry.delete_manifest(CACHE_REPOSITORY, cache_manifest)
            reclaimed = sweep_registry_blobs(registry.root, grace_seconds=2 * HOUR, writers_stopped=True)

            self.assertEqual(reclaimed.deleted_blobs, 3)
            self.assertTrue(registry.exists(shared))
            self.assertTrue(registry.exists(image_manifest))
            for value in (cache_manifest, cache_config, cache_only):
                self.assertFalse(registry.exists(value))


if __name__ == "__main__":
    unittest.main()
