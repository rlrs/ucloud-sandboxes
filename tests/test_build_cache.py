from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
from io import BytesIO
import json
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


def affinity_tag(value: str, *, affinity: str, age: int = 0, recipe: str = "recipe") -> str:
    recipe_hash = hashlib.sha256(recipe.encode()).hexdigest()[:16]
    identity = hashlib.sha256(value.encode()).hexdigest()[:32]
    return f"bc2-{recipe_hash}-{affinity[:32]}-{NOW - age:010d}-{identity}"


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
        self.assertEqual(plan.matching_ref, plan.imports[0])
        self.assertEqual(self.cache().prepare("unrelated-recipe").matching_ref, "")

    def test_concurrent_builds_export_to_unique_tags(self) -> None:
        cache = self.cache()
        with ThreadPoolExecutor(max_workers=8) as executor:
            plans = list(executor.map(cache.prepare, ["same recipe"] * 100))
        self.assertEqual(len({plan.export_ref for plan in plans}), 100)
        self.assertTrue(all(plan.imports == () for plan in plans))

    def test_exact_affinity_outside_recent_eight_precedes_recipe_and_diverse_fallbacks(self):
        affinity = hashlib.sha256(b"verified archive, dockerfile, args A").hexdigest()
        exact = affinity_tag("exact", affinity=affinity, age=100, recipe="wanted")
        newest_recipe = tag("newest-recipe", age=50, recipe="wanted")
        self.registry.add(exact, manifest("exact"))
        self.registry.add(newest_recipe, manifest("newest-recipe"))
        for number in range(60):
            self.registry.add(tag(f"burst-{number}", age=number, recipe="busy"), manifest(str(number)))
        diverse = affinity_tag("diverse", affinity="c" * 64, age=200, recipe="other")
        self.registry.add(diverse, manifest("diverse"))
        self.registry.add(tag("legacy-old", age=300, recipe="legacy"), manifest("legacy-old"))
        self.assertEqual(len(self.registry.tags_by_name), 64)
        plan = self.cache().prepare("wanted", affinity_key=affinity)
        refs = [value.rsplit(":", 1)[-1] for value in plan.imports]
        self.assertEqual(refs[:5], [exact, newest_recipe, tag("burst-0", recipe="busy"), diverse,
                                   tag("legacy-old", age=300, recipe="legacy")])
        self.assertEqual(len(refs), 8)
        self.assertEqual(len(set(refs)), 8)
        self.assertEqual(plan.matching_ref, plan.imports[0])
        self.assertTrue(plan.affinity_match)
        exported = plan.export_ref.rsplit(":", 1)[-1]
        self.assertEqual(len(exported), 97)
        self.assertTrue(exported.startswith("bc2-" + hashlib.sha256(b"wanted").hexdigest()[:16] + "-" + affinity[:32] + "-"))
        one = self.cache(import_limit=1).prepare("wanted", affinity_key=affinity)
        self.assertEqual(one.imports, (plan.imports[0],))

    def test_changed_affinity_or_recipe_cannot_claim_exact_match(self):
        old_affinity = hashlib.sha256(b"same archive and dockerfile; args A").hexdigest()
        new_affinity = hashlib.sha256(b"same archive and dockerfile; args B").hexdigest()
        older = affinity_tag("older", affinity=old_affinity, age=10)
        newer = tag("newer", age=1)
        wrong_recipe = affinity_tag("other", affinity=new_affinity, recipe="other-recipe")
        for name in (older, newer, wrong_recipe):
            self.registry.add(name, manifest(name))
        plan = self.cache().prepare("recipe", affinity_key=new_affinity)
        self.assertFalse(plan.affinity_match)
        self.assertTrue(plan.matching_ref.endswith(":" + newer))
        self.assertTrue(plan.imports[0].endswith(":" + newer))
        legacy = self.cache().prepare("recipe")
        self.assertFalse(legacy.affinity_match)
        self.assertIn(":bc1-", legacy.export_ref)

    def test_affinity_validation_and_expired_future_or_unknown_tags_do_not_become_hits(self):
        affinity = "a" * 64
        for name in (affinity_tag("expired", affinity=affinity, age=8 * 86400),
                     affinity_tag("future", affinity=affinity, age=-1),
                     affinity_tag("unknown", affinity=affinity).replace("bc2-", "bc3-")):
            self.registry.add(name, manifest(name))
        plan = self.cache().prepare("recipe", affinity_key=affinity)
        self.assertEqual(plan.imports, ())
        self.assertFalse(plan.affinity_match)
        for bad in (None, 1, "a" * 63, "A" * 64, "sha256:" + affinity, affinity + "\n"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.cache().prepare("recipe", affinity_key=bad)

    def test_concurrent_affinity_exports_are_unique_and_keep_input_hint(self):
        cache, affinity = self.cache(), "a" * 64
        with ThreadPoolExecutor(max_workers=8) as executor:
            plans = list(executor.map(lambda _: cache.prepare("same recipe", affinity_key=affinity), range(100)))
        self.assertEqual(len({plan.export_ref for plan in plans}), 100)
        self.assertTrue(all(":bc2-" in plan.export_ref and "-" + affinity[:32] + "-" in plan.export_ref
                            and not plan.affinity_match for plan in plans))

    def test_mixed_schema_pruning_preserves_shared_blobs_aliases_and_newest_owned_timestamp(self):
        oldest = affinity_tag("expired", affinity="a" * 64, age=8 * 86400)
        expired_digest = self.registry.add(oldest, manifest("expired", (("shared", 100),)))
        old_alias = tag("old-alias", age=9 * 86400)
        current_alias = affinity_tag("current-alias", affinity="b" * 64, age=1)
        retained_digest = self.registry.add(old_alias, manifest("aliased", (("shared", 100),)))
        self.registry.tags_by_name[current_alias] = retained_digest
        protected = affinity_tag("protected", affinity="c" * 64, age=10 * 86400)
        protected_digest = self.registry.add(protected, manifest("protected", (("shared", 100),)))
        self.registry.tags_by_name[protected.replace("bc2-", "bc3-")] = protected_digest
        result = self.cache().prune(execute=True)
        self.assertEqual(result["deleted_digests"], [expired_digest])
        self.assertEqual(result["candidate_tags"], [oldest])
        self.assertEqual(result["protected_entries"], 1)
        self.assertEqual(result["retained_bytes"], 110)
        self.assertEqual(result["retained_entries"], 3)

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


class CacheMountRegistry(RegistryClient):
    def __init__(self, document):
        super().__init__("http://registry:5000")
        self.document = document
        self.requests = []
        self.mounts = []
        self.on_mount = lambda _digest: True
        self.after_read = lambda: None
        self.header_digest = None

    def _request(self, path, **kwargs):
        self.requests.append((path, kwargs))
        payload = json.dumps(self.document, indent=2).encode()
        response = BytesIO(payload)
        response.headers = {"Docker-Content-Digest": self.header_digest or "sha256:" + hashlib.sha256(payload).hexdigest()}
        self.response = response
        self.after_read()
        return response

    def mount_blob(self, target, source, blob_digest, *, timeout_seconds=None):
        self.mounts.append((target, source, blob_digest, timeout_seconds))
        return self.on_mount(blob_digest)


class RegistryBuildCacheMountTests(unittest.TestCase):
    def setUp(self):
        self.document = manifest("cache", (("small", 3), ("large", 300), ("small", 3)))
        for layer in self.document["layers"]:
            layer["mediaType"] = "application/vnd.oci.image.layer.v1.tar+gzip"
        self.registry = CacheMountRegistry(self.document)
        self.cache = RegistryBuildCache(REF, client=self.registry, clock=lambda: NOW)
        self.source = f"registry:5000/{REPOSITORY}:{tag('source')}"
        self.target = "registry:5000/ucloud-managed/my-image:latest"

    def test_mounts_one_verified_snapshot_largest_first_without_duplicate_layers_or_config(self):
        result = self.cache.pre_mount(self.target, self.source)
        self.assertEqual([x[2] for x in self.registry.mounts], [digest("large"), digest("small")])
        self.assertTrue(all(x[:2] == ("ucloud-managed/my-image", REPOSITORY) for x in self.registry.mounts))
        self.assertTrue(all(0 < x[3] <= 3 for x in self.registry.mounts))
        self.assertEqual(result["mounted"], 2)
        self.assertEqual(result["mounted_descriptor_bytes"], 303)
        self.assertEqual(len(self.registry.requests), 1)
        self.assertTrue(self.registry.response.closed)
        self.assertEqual(self.registry.timeout_seconds, 30)

    def test_affinity_cache_tags_use_same_verified_mount_path(self):
        source = f"registry:5000/{REPOSITORY}:" + affinity_tag("source", affinity="a" * 64)
        result = self.cache.pre_mount(self.target, source)
        self.assertEqual(result["mounted"], 2)
        self.assertEqual(result["mounted_descriptor_bytes"], 303)
        self.assertEqual(len(self.registry.requests), 1)
        self.assertTrue(self.registry.response.closed)

    def test_unmanaged_cross_registry_and_malformed_destinations_do_not_make_requests(self):
        for target in (
            "other:5000/ucloud-managed/image:latest", "registry:5000/ordinary/image:latest",
            "registry:5000/ucloud-managed-evil/image:latest", "registry:5000/ucloud-managed:latest",
            "registry:5000/ucloud-managed/../image:latest", "registry:5000/ucloud-managed/image@" + digest("image"),
            "https://registry:5000/ucloud-managed/image:latest", "registry:5000/ucloud-managed/image:bad/tag",
        ):
            with self.subTest(target=target):
                self.assertTrue(self.cache.pre_mount(target, self.source)["skipped"])
        self.assertEqual(self.registry.requests, [])
        self.registry.base_url = "http://other-registry:5000"
        self.assertTrue(self.cache.pre_mount(self.target, self.source)["skipped"])
        self.assertEqual(self.registry.requests, [])

    def test_unselected_or_external_source_does_not_make_requests(self):
        for source in ("", REF, self.source.replace("registry:5000", "other:5000"), self.source.replace(REPOSITORY, "other/ucloud-build-cache")):
            with self.subTest(source=source):
                self.assertTrue(self.cache.pre_mount(self.target, source)["skipped"])
        self.assertEqual(self.registry.requests, [])

    def test_malformed_final_descriptor_prevents_all_mounts(self):
        for bad in (None, {}, {"digest": "sha256:bad", "size": 1},
                    {"digest": digest("bad"), "size": True},
                    {"digest": digest("bad"), "size": -1},
                    {"digest": digest("bad"), "size": 1, "mediaType": "unknown"},
                    {"digest": digest("large"), "size": 301, "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip"}):
            with self.subTest(bad=bad):
                self.registry.document = deepcopy(self.document)
                self.registry.document["layers"].append(bad)
                self.assertEqual(self.cache.pre_mount(self.target, self.source)["error"], "ValueError")
                self.assertEqual(self.registry.mounts, [])

    def test_wrong_manifest_digest_kind_config_and_layer_limit_prevent_mounts(self):
        mutations = (
            lambda d: d.update(schemaVersion=1),
            lambda d: d.update(mediaType="application/vnd.oci.image.index.v1+json"),
            lambda d: d["config"].update(mediaType="application/vnd.oci.image.config.v1+json"),
            lambda d: d["config"].update(digest="invalid"),
            lambda d: d.update(layers=d["layers"] * 22),
            lambda d: d.update(extra="x" * (256 * 1024)),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                self.registry.document = deepcopy(self.document)
                mutate(self.registry.document)
                self.assertEqual(self.cache.pre_mount(self.target, self.source)["error"], "ValueError")
                self.assertEqual(self.registry.mounts, [])
                self.assertTrue(self.registry.response.closed)
        self.registry.document = self.document
        self.registry.header_digest = digest("different-raw-bytes")
        self.assertEqual(self.cache.pre_mount(self.target, self.source)["error"], "ValueError")
        self.assertEqual(self.registry.mounts, [])

    def test_tag_change_after_snapshot_does_not_change_mounted_descriptors(self):
        # Simulate another writer replacing the tag immediately after the GET
        # response was captured. Only content from the verified snapshot is used.
        self.registry.after_read = lambda: setattr(self.registry, "document", manifest("new"))
        result = self.cache.pre_mount(self.target, self.source)
        self.assertEqual(result["mounted"], 2)
        self.assertEqual([x[2] for x in self.registry.mounts], [digest("large"), digest("small")])

    def test_pruned_source_mount_miss_is_optional(self):
        self.registry.on_mount = lambda _: False
        result = self.cache.pre_mount(self.target, self.source)
        self.assertEqual(result["attempted"], 2)
        self.assertEqual(result["mounted"], 0)
        self.assertNotIn("error", result)

    def test_partial_mount_failure_keeps_counts_and_stops_without_exposing_error_text(self):
        def fail_second(_digest):
            if len(self.registry.mounts) == 2:
                raise OSError("private transport details")
            return True
        self.registry.on_mount = fail_second
        result = self.cache.pre_mount(self.target, self.source)
        self.assertEqual(result["mounted"], 1)
        self.assertEqual(result["mounted_descriptor_bytes"], 300)
        self.assertEqual(result["error"], "OSError")
        self.assertNotIn("private", json.dumps(result))

    def test_deadline_passes_remaining_budget_and_stops_before_next_layer(self):
        now = [10.0]
        def delay(_digest):
            now[0] += 3.1
            return True
        self.registry.on_mount = delay
        with patch("ucloud_sandboxes.build_cache.time.monotonic", side_effect=lambda: now[0]):
            result = self.cache.pre_mount(self.target, self.source)
        self.assertEqual(len(self.registry.mounts), 1)
        self.assertEqual(self.registry.mounts[0][3], 3.0)
        self.assertEqual(result["error"], "TimeoutError")
        self.assertEqual(result["mounted"], 1)

    def test_concurrent_targets_keep_repository_and_timeout_state_independent(self):
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(
                lambda n: self.cache.pre_mount(f"registry:5000/ucloud-managed/image-{n}:latest", self.source), range(12)))
        self.assertTrue(all(result["mounted"] == 2 for result in results))
        self.assertEqual({call[0] for call in self.registry.mounts}, {f"ucloud-managed/image-{n}" for n in range(12)})
        self.assertEqual(self.registry.timeout_seconds, 30)


if __name__ == "__main__":
    unittest.main()
