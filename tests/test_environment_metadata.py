"""Signed metadata hints carried without breaking mixed-version fleets (C2.2)."""
import json
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from tests.test_environment_artifact import EnvironmentArtifactTests
from ucloud_sandboxes.build_deadline import ImageBuildTimeoutError, build_execution_deadline
from ucloud_sandboxes.environment_artifact import (
    CHUNK_BYTES, EnvironmentArtifactRegistry, EnvironmentComponent, canonical_bytes, content_digest, sign_component,
)
from ucloud_sandboxes.environment_builder import FreshEnvironmentBuilder, publication_metrics
from ucloud_sandboxes.environment_metadata import (
    METADATA_ANNOTATION, MetadataHint, component_digest, read_metadata_hint, sign_hint, sign_metadata_hint,
)


def image_of(path: Path, chunks: int):
    path.write_bytes(b"".join(bytes([index % 251]) * CHUNK_BYTES for index in range(chunks)))
    return path


class MetadataHintTests(EnvironmentArtifactTests):
    def setUp(self):
        super().setUp()
        self.big = image_of(self.root / "big.erofs", 12)
        self.big_component = sign_component(self.big, source_image="sha256:" + "3" * 64, signing_key=self.key)
        self.hint = sign_hint(self.big_component, [(0, 4096), (3, 100), (4, 90000), (7, 8192), (11, 1)], self.key)

    def manifest(self, digest):
        return json.loads(self.client.manifests[digest])

    def test_annotation_is_the_only_manifest_change_and_config_bytes_are_identical(self):
        plain = self.registry.publish(self.big, self.big_component, tag="plain")
        hinted = self.registry.publish(self.big, self.big_component, tag="hinted", metadata=self.hint)
        self.assertNotEqual(plain, hinted)
        with_hint, without = self.manifest(hinted), self.manifest(plain)
        self.assertEqual(set(with_hint) - set(without), {"annotations"})
        self.assertEqual(with_hint["annotations"], {METADATA_ANNOTATION: self.hint.encode()})
        self.assertEqual({key: value for key, value in with_hint.items() if key != "annotations"}, without)
        # The signed index old workers parse with an exact key set is the same blob.
        config = self.client.blobs[with_hint["config"]["digest"]]
        self.assertEqual(EnvironmentComponent.from_dict(json.loads(config)), self.big_component)
        self.assertLess(len(self.client.manifests[hinted]), 4096)

    def test_workers_load_hinted_components_and_verify_the_hint(self):
        hinted = self.registry.publish(self.big, self.big_component, tag="hinted", metadata=self.hint)
        reader = EnvironmentArtifactRegistry(self.client, "environments", self.registry.trusted_keys)
        self.assertEqual(reader.load(hinted), self.big_component)
        self.assertTrue(reader.whole_image(self.big_component))
        self.assertEqual(reader.metadata_hint(hinted), ("present", self.hint))
        self.assertEqual(reader.load(self.digest), self.component)  # Published before hints existed.
        self.assertEqual(reader.metadata_hint(self.digest), ("absent", None))
        self.assertEqual(reader.metadata_hint("sha256:" + "9" * 64), ("absent", None))

    def test_registry_retains_only_the_most_recently_loaded_hints(self):
        from ucloud_sandboxes import environment_artifact
        hinted = self.registry.publish(self.big, self.big_component, tag="hinted", metadata=self.hint)
        other_hint = sign_hint(self.big_component, [(0, 1)], self.key)
        other = self.registry.publish(self.big, self.big_component, tag="other", metadata=other_hint)
        reader = EnvironmentArtifactRegistry(self.client, "environments", self.registry.trusted_keys)
        with patch.object(environment_artifact, "_RETAINED_HINTS", 2):
            for digest in (hinted, self.digest, hinted, other):  # A reload is the newest again.
                reader.load(digest)
        self.assertEqual(reader.metadata_hint(hinted), ("present", self.hint))
        self.assertEqual(reader.metadata_hint(other), ("present", other_hint))
        self.assertEqual(list(reader._metadata), [hinted, other])

    def test_an_unverifiable_hint_never_rejects_its_component(self):
        other_key = Ed25519PrivateKey.generate()
        other_public = other_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        trusted = self.registry.trusted_keys | {content_digest(other_public): other_public}
        reader = EnvironmentArtifactRegistry(self.client, "environments", trusted)
        foreign = sign_hint(self.component, [(0, 10)], self.key)
        tampered = json.loads(self.hint.encode()) | {"chunks": [[0, 4096], [5, 100]], "metadata_bytes": 4196}
        values = {
            "garbage": "not json",
            "deep": "[" * 100000,
            "another component": foreign.encode(),
            "tampered": canonical_bytes(tampered).decode(),
            "another producer": sign_hint(self.big_component, [(0, 10)], other_key).encode(),
            "future schema": canonical_bytes(json.loads(self.hint.encode()) | {"schema": "v2"}).decode(),
            "extra field": canonical_bytes(json.loads(self.hint.encode()) | {"new": 1}).decode(),
            "oversized": "x" * (600 * 1024),
            "not a string": 7,
        }
        config = canonical_bytes(self.big_component.to_dict())
        self.client.blobs[content_digest(config)] = config
        self.client.blobs[self.big_component.image_digest] = self.big.read_bytes()
        for name, value in values.items():
            with self.subTest(name=name):
                document = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                            "config": {"mediaType": "application/vnd.ucloud.environment.erofs.v1+json",
                                       "digest": content_digest(config), "size": len(config)},
                            "layers": [{"mediaType": "application/vnd.ucloud.environment.image.v1",
                                        "digest": self.big_component.image_digest,
                                        "size": self.big_component.image_size}],
                            "annotations": {METADATA_ANNOTATION: value, "org.example.other": "kept"}}
                digest = content_digest(canonical_bytes(document))
                with self.assertLogs("ucloud_sandboxes.environment_metadata", "WARNING"):
                    self.assertEqual(reader.load_document(digest, document), self.big_component)
                self.assertEqual(reader.metadata_hint(digest), ("unsupported", None))
        # The published binding check refuses a hint for another component.
        with self.assertRaisesRegex(ValueError, "another component"):
            self.registry.publish(self.big, self.big_component, tag="wrong", metadata=foreign)
        self.assertEqual(read_metadata_hint({"annotations": []}, "", None, {}), ("absent", None))

    def test_strict_fields_and_bounds(self):
        raw = json.loads(self.hint.encode())
        for change in ({"chunks": [[3, 100], [0, 4096]]}, {"chunks": [[0, 0]]}, {"chunks": [[12, 1]]},
                       {"chunks": [[0, CHUNK_BYTES + 1]]}, {"chunks": [[True, 1]]}, {"chunks": []},
                       {"complete": 1}, {"chunk_count": 0}, {"metadata_bytes": 1}, {"walker": "other"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                MetadataHint.decode(canonical_bytes(raw | change).decode())
        self.assertEqual(MetadataHint.decode(self.hint.encode()), self.hint)
        self.assertEqual(self.hint.metadata_bytes, 4096 + 100 + 90000 + 8192 + 1)
        self.assertTrue(self.hint.complete)
        self.assertEqual(self.hint.component, component_digest(self.big_component))

    def test_prefetch_order_takes_densest_chunks_within_budget_superblock_first(self):
        self.assertEqual(self.hint.prefetch_order(max_bytes=3 * CHUNK_BYTES, max_chunks=99), (0, 4, 7))
        self.assertEqual(self.hint.prefetch_order(max_bytes=99 * CHUNK_BYTES, max_chunks=2), (0, 4))
        self.assertEqual(self.hint.prefetch_order(max_bytes=CHUNK_BYTES - 1, max_chunks=99), ())
        self.assertEqual(self.hint.prefetch_order(max_bytes=99 * CHUNK_BYTES, max_chunks=99), (0, 3, 4, 7, 11))

    def test_oversized_hint_keeps_the_densest_chunks_and_says_so(self):
        from ucloud_sandboxes import environment_metadata
        with patch.object(environment_metadata, "MAX_HINT_CHUNKS", 3):
            hint = sign_hint(self.big_component, [(0, 1), (3, 100), (4, 90000), (7, 8192), (11, 50)], self.key)
        self.assertFalse(hint.complete)
        self.assertEqual(hint.chunks, ((0, 1), (4, 90000), (7, 8192)))


class BuilderHintTests(EnvironmentArtifactTests):
    def builder(self):
        return FreshEnvironmentBuilder(None, self.registry, self.key, self.root / "work")

    def test_non_erofs_bytes_publish_the_unchanged_hint_free_manifest(self):
        with publication_metrics() as metrics:
            digest = self.builder()._publish_component(self.image, self.component, "fixture")
        self.assertEqual(digest, self.digest)
        self.assertEqual(metrics["metadata_hint_unsupported"], 1)
        self.assertNotIn("annotations", json.loads(self.client.manifests[digest]))

    def test_expired_build_deadline_is_not_mistaken_for_an_unsupported_image(self):
        expired = ImageBuildTimeoutError("image build exceeded its server execution deadline")
        with patch("ucloud_sandboxes.environment_builder.remaining_build_execution_seconds", side_effect=expired), \
             patch("ucloud_sandboxes.erofs_metadata.walk", side_effect=lambda *a, check, **k: check()), \
             self.assertRaises(ImageBuildTimeoutError):
            self.builder()._publish_component(self.image, self.component, "fixture")
        self.assertEqual(list(self.client.manifests), [self.digest])
        with build_execution_deadline(60):
            self.assertEqual(self.builder()._publish_component(self.image, self.component, "fixture"), self.digest)

    @unittest.skipUnless(shutil.which("mkfs.erofs"), "mkfs.erofs (erofs-utils) is not installed")
    def test_real_erofs_publication_carries_a_verified_complete_hint(self):
        from tests.test_erofs_metadata import builder_image
        view = self.root / "view"
        (view / "etc").mkdir(parents=True)
        for index in range(200):
            (view / "etc" / f"file-{index}").write_text("x" * index)
        image = builder_image(view, self.root / "real.erofs")
        component = sign_component(image, source_image="sha256:" + "4" * 64, signing_key=self.key)
        with publication_metrics() as metrics:
            digest = self.builder()._publish_component(image, component, "real")
        self.assertEqual(metrics["metadata_hints"], 1)
        reader = EnvironmentArtifactRegistry(self.client, "environments", self.registry.trusted_keys)
        reader.load(digest)
        status, hint = reader.metadata_hint(digest)
        self.assertEqual(status, "present")
        expected, walked = sign_metadata_hint(image, component, self.key)
        self.assertEqual(hint, expected)
        self.assertTrue(hint.complete)
        self.assertEqual(hint.metadata_bytes, walked.metadata_bytes)
        self.assertEqual(metrics["metadata_hint_bytes"], walked.metadata_bytes)


if __name__ == "__main__":
    unittest.main()
