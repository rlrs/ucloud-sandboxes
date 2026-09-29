from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import unittest
from unittest.mock import patch

from ucloud_sandboxes.build_cache import (
    CACHE_CONFIG_MEDIA_TYPE,
    CACHE_MANIFEST_MEDIA_TYPE,
    RegistryBuildCache,
)
from ucloud_sandboxes.managed_registry import RegistryClient, RegistryRequestError


NOW = 1_790_726_400
REPOSITORY = "test/ucloud-build-cache"
REF = f"registry:5000/{REPOSITORY}:shared"


def digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def tag(value: str, *, age: int = 0, recipe: str = "recipe") -> str:
    recipe_hash = hashlib.sha256(recipe.encode()).hexdigest()[:16]
    identity = hashlib.sha256(value.encode()).hexdigest()[:32]
    return f"bc1-{recipe_hash}-{NOW - age:010d}-{identity}"


def manifest(value: str, layers: tuple[tuple[str, int], ...] = ()) -> dict:
    return {
        "schemaVersion": 2,
        "mediaType": CACHE_MANIFEST_MEDIA_TYPE,
        "config": {"mediaType": CACHE_CONFIG_MEDIA_TYPE, "digest": digest(f"config-{value}"), "size": 5},
        "layers": [{"digest": digest(name), "size": size} for name, size in layers],
    }


class FakeRegistry(RegistryClient):
    def __init__(self) -> None:
        super().__init__("http://registry:5000")
        self.tags_by_name: dict[str, str] = {}
        self.manifests: dict[str, dict] = {}
        self.deleted: list[tuple[str, str]] = []
        self.repositories: list[str] = []
        self.inventory_calls = 0
        self.on_inventory = None
        self.inventory_payload = None
        self.inventory_headers: dict[str, str] = {}
        self.missing = False

    def add(self, name: str, document: dict, *, identity: str | None = None) -> str:
        result = digest(identity or name)
        self.tags_by_name[name] = result
        self.manifests[result] = deepcopy(document)
        return result

    def _json_request(self, path: str, **kwargs):
        self.inventory_calls += 1
        if self.on_inventory:
            self.on_inventory(self)
        if self.missing:
            raise RegistryRequestError(404, "GET", path, "missing")
        if not path.startswith(f"/v2/{REPOSITORY}/tags/list?"):
            raise AssertionError(f"unexpected cache inventory path: {path}")
        payload = self.inventory_payload
        if payload is None:
            payload = {"name": REPOSITORY, "tags": list(self.tags_by_name)}
        return deepcopy(payload), self.inventory_headers

    def manifest_digest(self, repository: str, reference: str) -> str:
        self.repositories.append(repository)
        return self.tags_by_name[reference]

    def manifest_document(self, repository: str, reference: str):
        self.repositories.append(repository)
        return deepcopy(self.manifests[reference]), {"Docker-Content-Digest": reference}

    def delete_manifest(self, repository: str, reference: str) -> None:
        self.repositories.append(repository)
        self.deleted.append((repository, reference))


class RegistryBuildCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = FakeRegistry()

    def cache(self, **kwargs) -> RegistryBuildCache:
        return RegistryBuildCache(REF, client=self.registry, clock=lambda: NOW, **kwargs)

    def test_missing_repository_is_an_empty_cache(self) -> None:
        self.registry.missing = True
        plan = self.cache().prepare("recipe")
        self.assertEqual(plan.imports, ())
        self.assertTrue(plan.export_ref.startswith(f"registry:5000/{REPOSITORY}:bc1-"))
        self.assertEqual(self.cache().prune(execute=True)["deleted_digests"], [])

    def test_recent_recipe_cache_precedes_newer_shared_caches(self) -> None:
        for number in range(12):
            name = tag(str(number), age=number, recipe=f"recipe-{number}")
            self.registry.add(name, manifest(str(number)))
        self.registry.add("shared", manifest("external"))
        self.registry.add(tag("expired", age=8 * 86400), manifest("expired"))
        plan = self.cache().prepare("recipe-10")
        self.assertEqual(len(plan.imports), 8)
        self.assertEqual(plan.imports[0], f"registry:5000/{REPOSITORY}:{tag('10', age=10, recipe='recipe-10')}")
        self.assertEqual(plan.imports[1], f"registry:5000/{REPOSITORY}:{tag('0', recipe='recipe-0')}")
        self.assertTrue(all(":bc1-" in reference for reference in plan.imports))
        self.assertNotIn("expired", " ".join(plan.imports))

    def test_concurrent_builds_export_to_unique_tags(self) -> None:
        cache = self.cache()
        with ThreadPoolExecutor(max_workers=8) as executor:
            plans = list(executor.map(cache.prepare, ["same recipe"] * 100))
        self.assertEqual(len({plan.export_ref for plan in plans}), 100)
        self.assertTrue(all(plan.imports == () for plan in plans))

    def test_budget_counts_shared_blobs_once_and_config_blobs_too(self) -> None:
        expected_old = ""
        for number in range(3):
            result = self.registry.add(
                tag(str(number), age=number),
                manifest(str(number), (("shared", 100), (f"unique-{number}", 20))),
            )
            if number == 2:
                expected_old = result
        result = self.cache(max_bytes=150).prune(execute=True)
        self.assertEqual(result["retained_bytes"], 150)
        self.assertEqual(result["retained_entries"], 2)
        self.assertEqual(result["deleted_digests"], [expected_old])
        self.assertEqual(result["deleted_manifests"], 1)
        self.assertEqual(self.registry.deleted, [(REPOSITORY, expected_old)])

    def test_entry_and_age_limits_retain_only_recent_entries(self) -> None:
        fresh = tag("fresh")
        previous = tag("previous", age=1)
        expired = tag("expired", age=8 * 86400)
        for name in (fresh, previous, expired):
            self.registry.add(name, manifest(name))
        result = self.cache(max_entries=1).prune()
        self.assertEqual(result["candidate_tags"], sorted([previous, expired]))
        self.assertEqual(result["retained_entries"], 1)
        self.assertEqual(self.registry.deleted, [])

    def test_unknown_alias_protects_cache_digest_even_over_budget(self) -> None:
        name = tag("old", age=8 * 86400)
        reference = self.registry.add(name, manifest("old", (("large", 100),)))
        self.registry.tags_by_name["external-cache-owner"] = reference
        result = self.cache(max_bytes=1).prune(execute=True)
        self.assertEqual(result["protected_entries"], 1)
        self.assertEqual(result["retained_bytes"], 105)
        self.assertEqual(self.registry.deleted, [])

    def test_recent_owned_alias_protects_expired_alias(self) -> None:
        reference = self.registry.add(tag("old", age=8 * 86400), manifest("old"))
        self.registry.tags_by_name[tag("fresh")] = reference
        result = self.cache().prune(execute=True)
        self.assertEqual(result["retained_entries"], 2)
        self.assertEqual(result["retained_bytes"], 5)
        self.assertEqual(self.registry.deleted, [])

    def test_all_expired_owned_aliases_are_deleted_as_one_manifest(self) -> None:
        reference = self.registry.add(tag("old", age=9 * 86400), manifest("old"))
        self.registry.tags_by_name[tag("also-old", age=8 * 86400)] = reference
        result = self.cache().prune(execute=True)
        self.assertEqual(len(result["candidate_tags"]), 2)
        self.assertEqual(self.registry.deleted, [(REPOSITORY, reference)])

    def test_external_images_are_never_deleted_or_read_as_cache_manifests(self) -> None:
        self.registry.add("external-image", {"invalid-as-cache": True})
        reference = self.registry.add(tag("old", age=8 * 86400), manifest("old"))
        self.cache().prune(execute=True)
        self.assertEqual(self.registry.deleted, [(REPOSITORY, reference)])
        self.assertEqual(set(self.registry.repositories), {REPOSITORY})

    def test_ordinary_image_under_owned_tag_stops_all_deletion(self) -> None:
        self.registry.add(tag("old", age=8 * 86400), manifest("old"))
        document = manifest("image")
        document["config"]["mediaType"] = "application/vnd.oci.image.config.v1+json"
        self.registry.add(tag("image"), document)
        with self.assertRaisesRegex(ValueError, "BuildKit cache manifest"):
            self.cache().prune(execute=True)
        self.assertEqual(self.registry.deleted, [])

    def test_conflicting_blob_sizes_stop_all_deletion(self) -> None:
        self.registry.add(tag("one", age=8 * 86400), manifest("one", (("shared", 10),)))
        self.registry.add(tag("two"), manifest("two", (("shared", 20),)))
        with self.assertRaisesRegex(ValueError, "disagree"):
            self.cache().prune(execute=True)
        self.assertEqual(self.registry.deleted, [])

    def test_invalid_descriptors_stop_all_deletion(self) -> None:
        for value in (None, {}, {"digest": "sha256:bad", "size": 1}, {"digest": digest("a"), "size": True}, {"digest": digest("a"), "size": -1}):
            with self.subTest(value=value):
                self.registry = FakeRegistry()
                document = manifest("bad")
                document["layers"] = [value]
                self.registry.add(tag("bad", age=8 * 86400), document)
                with self.assertRaises(ValueError):
                    self.cache().prune(execute=True)
                self.assertEqual(self.registry.deleted, [])

    def test_new_alias_during_inventory_defers_all_deletion(self) -> None:
        reference = self.registry.add(tag("old", age=8 * 86400), manifest("old"))

        def publish_alias(registry):
            if registry.inventory_calls == 2:
                registry.tags_by_name["external-image"] = reference

        self.registry.on_inventory = publish_alias
        with self.assertRaisesRegex(ValueError, "inventory changed"):
            self.cache().prune(execute=True)
        self.assertEqual(self.registry.deleted, [])

    def test_new_digest_during_inventory_is_never_deleted(self) -> None:
        self.registry.add(tag("old", age=8 * 86400), manifest("old"))

        def publish_cache(registry):
            if registry.inventory_calls == 2:
                registry.add(tag("new"), manifest("new"))

        self.registry.on_inventory = publish_cache
        with self.assertRaisesRegex(ValueError, "inventory changed"):
            self.cache().prune(execute=True)
        self.assertEqual(self.registry.deleted, [])

    def test_tag_changed_after_second_inventory_is_not_deleted(self) -> None:
        name = tag("old", age=8 * 86400)
        self.registry.add(name, manifest("old"))
        original = self.registry.manifest_digest
        reads = 0

        def retarget(repository, reference):
            nonlocal reads
            reads += 1
            if reads == 3:
                return digest("replacement")
            return original(repository, reference)

        with patch.object(self.registry, "manifest_digest", retarget):
            with self.assertRaisesRegex(ValueError, "tag changed"):
                self.cache().prune(execute=True)
        self.assertEqual(self.registry.deleted, [])

    def test_partial_delete_failure_preserves_gc_accounting(self) -> None:
        for number in range(3):
            self.registry.add(tag(str(number), age=number), manifest(str(number)))
        original = self.registry.delete_manifest

        def fail_after_first(repository, reference):
            if self.registry.deleted:
                raise OSError("registry unavailable")
            original(repository, reference)

        with patch.object(self.registry, "delete_manifest", fail_after_first):
            result = self.cache(max_entries=1).prune(execute=True)
        self.assertEqual(result["deleted_manifests"], 1)
        self.assertEqual(len(result["deleted_digests"]), 1)
        self.assertEqual(result["error"], "OSError")
        self.assertTrue(result["incomplete"])

    def test_prepare_abandons_slow_paginated_inventory(self) -> None:
        self.registry.inventory_headers = {"Link": f'</v2/{REPOSITORY}/tags/list?n=1000&last=next>; rel="next"'}
        with patch("ucloud_sandboxes.build_cache.time.monotonic", side_effect=[0.0, 0.0, 4.0]):
            with self.assertRaises(TimeoutError):
                self.cache().prepare("recipe")
        self.assertEqual(self.registry.inventory_calls, 1)

    def test_malformed_and_oversized_inventory_is_fail_closed(self) -> None:
        for payload in (
            {"name": REPOSITORY},
            {"name": "other-repository", "tags": []},
            {"name": REPOSITORY, "tags": ["valid", None]},
            {"name": REPOSITORY, "tags": ["duplicate", "duplicate"]},
            {"name": REPOSITORY, "tags": "invalid"},
        ):
            with self.subTest(payload=payload):
                self.registry.inventory_payload = payload
                with self.assertRaises(ValueError):
                    self.cache().prune(execute=True)
                self.assertEqual(self.registry.deleted, [])
        self.registry.inventory_payload = {"name": REPOSITORY, "tags": ["a", "b"]}
        with patch("ucloud_sandboxes.build_cache.MAX_CACHE_INVENTORY_TAGS", 1):
            with self.assertRaisesRegex(ValueError, "tag limit"):
                self.cache().prune(execute=True)
        self.assertEqual(self.registry.deleted, [])

    def test_incomplete_pagination_does_not_delete(self) -> None:
        self.registry.add(tag("old", age=8 * 86400), manifest("old"))
        self.registry.inventory_headers = {"Link": f'</v2/{REPOSITORY}/tags/list?n=1000&last=next>; rel="next"'}

        def disappear(registry):
            if registry.inventory_calls == 2:
                registry.missing = True

        self.registry.on_inventory = disappear
        with self.assertRaises(RegistryRequestError):
            self.cache().prune(execute=True)
        self.assertEqual(self.registry.deleted, [])

    def test_cache_repository_is_required_before_any_request(self) -> None:
        for reference in (
            "registry:5000/environments:tag",
            "registry:5000/ucloud-build-cache/other:tag",
            "registry:5000/ucloud-build-cache@sha256:123",
            "https://registry/ucloud-build-cache",
            "registry:5000/../ucloud-build-cache",
            "registry:5000/ucloud-build-cache:invalid/tag",
        ):
            with self.subTest(reference=reference):
                with self.assertRaises(ValueError):
                    RegistryBuildCache(reference, client=self.registry)
        self.assertEqual(self.registry.inventory_calls, 0)

    def test_policy_values_and_import_fanout_are_bounded(self) -> None:
        for kwargs in ({"max_bytes": 0}, {"max_age_seconds": -1}, {"max_entries": True}, {"import_limit": 9}):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    self.cache(**kwargs)


if __name__ == "__main__":
    unittest.main()
