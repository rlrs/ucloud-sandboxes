from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from tests.test_environment_artifact import EnvironmentArtifactTests
from ucloud_sandboxes.environment_artifact import OCI_IMAGE, attach_environment_to_image, canonical_bytes, publish_environment
from ucloud_sandboxes.environment_dependencies import EnvironmentDependencyResolver
from ucloud_sandboxes.environment_manifest import EnvironmentManifest

from ucloud_sandboxes.control_plane import _persist_registry_image_protection, _release_registry_reference_keys
from ucloud_sandboxes.managed_registry import RegistryUsageStore, digest_protection_tag


class EnvironmentDependencyTests(unittest.TestCase):
    def test_exact_owned_closure_survives_changed_source_and_releases_once(self):
        with TemporaryDirectory() as temporary:
            store = RegistryUsageStore(Path(temporary) / "usage.sqlite")
            image_digest = "sha256:" + "1" * 64
            component = "sha256:" + "2" * 64
            root = "sha256:" + "3" * 64
            other = "sha256:" + "4" * 64
            image = "registry.example/source:latest@" + image_digest
            references = tuple(("environments", digest_protection_tag(digest), digest) for digest in (component, root))
            ensured = []
            class Resolver:
                def __call__(self, image):
                    return references
                def ensure_reference(self, *reference):
                    ensured.append(reference)
            resolver = Resolver()
            _persist_registry_image_protection(store, image, "route-owner", touch=True, persistent=True,
                                               dependency_resolver=resolver)
            self.assertEqual(tuple(ensured), references)
            ensured.clear()
            _persist_registry_image_protection(store, image, "route-owner", touch=False, persistent=True,
                                               dependency_resolver=resolver)
            self.assertEqual(ensured, [])
            for repository, tag, digest in references:
                self.assertEqual(store.get_lease(repository, tag, "route-owner:environment").digest, digest)
            store.acquire_reference("environments", digest_protection_tag(component), "other-owner", digest=component)
            # Source selection can change; release uses persisted owner rows.
            references = (("environments", digest_protection_tag(other), other),)
            _release_registry_reference_keys(store, {("source", "latest", "route-owner")}, image_owners=frozenset({"route-owner"}))
            for digest in (component, root):
                self.assertIsNone(store.get_lease("environments", digest_protection_tag(digest), "route-owner:environment"))
            self.assertIsNotNone(store.get_lease("environments", digest_protection_tag(component), "other-owner"))
            self.assertEqual(store.release_owner("route-owner:environment"), 0)
            # A later claim must recreate protection tags even though closure
            # resolution may be cached from before the original owner's GC.
            _persist_registry_image_protection(store, image, "route-owner", touch=False, persistent=True,
                                               dependency_resolver=resolver)
            self.assertEqual(tuple(ensured), references)


if __name__ == "__main__":
    unittest.main()


class EnvironmentCachedDependencyTests(EnvironmentArtifactTests):
    def test_cached_closure_recreates_tags_after_owner_release_and_registry_gc(self):
        root = publish_environment(self.registry, source_image=self.component.source_image,
            environment=EnvironmentManifest(self.digest), image_config={}, signing_key=self.key, tag="root")
        self.client.manifests["image"] = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE,
            "config": {"digest": self.component.source_image}, "layers": []})
        annotated = attach_environment_to_image(self.registry, image_repository="source",
            image_reference="image", environment_digest=root)
        image = "registry.example/source:latest@" + annotated
        resolver = EnvironmentDependencyResolver(self.registry)
        store = RegistryUsageStore(self.root / "usage.sqlite")
        tags = {}
        def ensure(repository, digest):
            self.assertIn(digest, self.client.manifests)
            tags[(repository, digest_protection_tag(digest))] = digest
        self.client.ensure_digest_protection_tag = ensure
        _persist_registry_image_protection(store, image, "first", touch=True,
            persistent=True, dependency_resolver=resolver)
        self.assertEqual(set(tags.values()), {root, self.digest})
        store.release_owner("first:environment")
        tags.clear()  # Registry deletes expired protection tags before the new claim.
        with patch("ucloud_sandboxes.environment_dependencies.load_image_environment",
                   side_effect=AssertionError("immutable closure should already be cached")):
            _persist_registry_image_protection(store, image, "second", touch=True,
                persistent=True, dependency_resolver=resolver)
        self.assertEqual(set(tags.values()), {root, self.digest})
        self.assertTrue(all(store.get_lease(repository, tag, "second:environment") is not None
                            for repository, tag in tags))
