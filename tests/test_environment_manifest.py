from dataclasses import replace
import hashlib
import unittest

from ucloud_sandboxes.environment_manifest import (
    DOCKER_OVERLAY2_ABI,
    EnvironmentManifest,
)


class EnvironmentManifestTests(unittest.TestCase):
    def test_round_trip_preserves_component_order(self):
        manifest = EnvironmentManifest(
            base="sha256:" + "a" * 64,
            workspace="sha256:" + "b" * 64,
            toolkits=("sha256:" + "c" * 64, "sha256:" + "d" * 64),
        )
        self.assertEqual(EnvironmentManifest.from_dict(manifest.to_dict()), manifest)
        self.assertNotEqual(
            replace(manifest, toolkits=tuple(reversed(manifest.toolkits))).sha256,
            manifest.sha256,
        )
        self.assertNotEqual(replace(manifest, workspace=None).sha256, manifest.sha256)

    def test_docker_identity_preserves_checkpoint_fingerprint(self):
        image_id = "sha256:" + "a" * 64
        manifest = EnvironmentManifest(base=image_id)
        self.assertEqual(
            manifest.rootfs_fingerprint(DOCKER_OVERLAY2_ABI),
            hashlib.sha256(
                b"ucloud-overlay2-rootfs-v1\0" + image_id.encode("ascii")
            ).hexdigest(),
        )
        self.assertNotEqual(
            manifest.sha256, manifest.rootfs_fingerprint(DOCKER_OVERLAY2_ABI)
        )

    def test_unqualified_composition_cannot_be_used_as_a_docker_rootfs(self):
        manifest = EnvironmentManifest(base="sha256:" + "a" * 64)
        with self.assertRaisesRegex(ValueError, "unqualified"):
            manifest.rootfs_fingerprint("erofs-v1")
        for composed in (
            replace(manifest, workspace="sha256:" + "b" * 64),
            replace(manifest, toolkits=("sha256:" + "b" * 64,)),
        ):
            with self.assertRaisesRegex(ValueError, "already-composed"):
                composed.rootfs_fingerprint(DOCKER_OVERLAY2_ABI)

    def test_decoder_rejects_unknown_and_mutable_identity(self):
        valid = EnvironmentManifest(base="sha256:" + "a" * 64).to_dict()
        for bad in (
            {**valid, "base": "image:latest"},
            {**valid, "schema": True},
            {**valid, "schema": 2},
            {**valid, "composition": "bind-mounts"},
            {**valid, "toolkits": "sha256:" + "b" * 64},
            {**valid, "unexpected": 1},
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                EnvironmentManifest.from_dict(bad)
