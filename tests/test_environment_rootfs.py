import json
from pathlib import Path
from types import SimpleNamespace
import unittest

from tests.test_environment_artifact import EnvironmentArtifactTests
from ucloud_sandboxes.environment_artifact import (ENVIRONMENT_ANNOTATION, OCI_IMAGE,
    attach_environment_to_image, canonical_bytes, load_environment, publish_environment)
from ucloud_sandboxes.environment_manifest import EnvironmentManifest, HOST_EROFS_ABI
from ucloud_sandboxes.environment_rootfs import EnvironmentImageRuntime, EnvironmentRootfsStore
from ucloud_sandboxes.images import ImageManager, ImageStore
from ucloud_sandboxes.image_rootfs import OverlayRootfsManager


class EnvironmentRootfsTests(EnvironmentArtifactTests):
    def test_signed_composition_and_existing_oci_image_input(self):
        manifest = EnvironmentManifest(self.digest)
        root = publish_environment(self.registry, source_image=self.component.source_image,
            environment=manifest, image_config={"Cmd": ["/bin/sh"], "Env": ["A=B"]}, signing_key=self.key, tag="root")
        self.assertEqual(load_environment(self.registry, root).components, (self.digest,))
        original = {"schemaVersion": 2, "mediaType": OCI_IMAGE,
            "config": {"digest": self.component.source_image}, "layers": [{"digest": "sha256:" + "9" * 64}]}
        self.client.manifests["image"] = canonical_bytes(original)
        annotated = attach_environment_to_image(self.registry, image_repository="environments", image_reference="image", environment_digest=root)
        actual = json.loads(self.client.manifests[annotated])
        self.assertEqual(actual["config"], original["config"])
        self.assertEqual(actual["layers"], original["layers"])
        self.assertEqual(actual["annotations"][ENVIRONMENT_ANNOTATION], root)
        self.client.base_url = "http://localhost:5000"
        mounts = set()
        mount_commands = []
        class Runner:
            def run(self, command, **kwargs):
                if command[0] == "mount":
                    mount_commands.append(command)
                    if "overlay" in command:
                        lowerdirs = command[command.index("-o") + 1].split("lowerdir=", 1)[1]
                        if ":" not in lowerdirs:
                            raise AssertionError("Linux rejects a single lower with no upper")
                    mounts.add(Path(command[-1]))
                elif command[0] == "umount":
                    mounts.remove(Path(command[-1]))
                code = int(Path(command[-1]) not in mounts) if command[0] == "mountpoint" else 0
                return SimpleNamespace(returncode=code, stdout="", stderr="")
        calls = []
        backend = SimpleNamespace(ensure=lambda digest: calls.append(digest) or (self.root / "components" / digest[7:]),
                                  drop=lambda digest: True)
        store = EnvironmentRootfsStore(self.root / "store", self.registry, backend, runner=Runner(), referenced=lambda _: False)
        ref = "localhost:5000/environments:image@" + annotated
        manager = ImageManager(ImageStore(self.root / "image-api.sqlite"), EnvironmentImageRuntime(store))
        record, pulled = manager.pull(ref, image_id="fixture")
        self.assertEqual(mount_commands[0][1], "--bind")
        self.assertEqual(record.manifest_digest, annotated)
        self.assertEqual(pulled.argv[0], "immutable-environment")
        with store.operation_lease(ref) as image:
            self.assertEqual(image.environment, manifest)
            self.assertEqual(image.backend_abi, HOST_EROFS_ABI)
            self.assertEqual(image.image_config.command, ("/bin/sh",))
            image_id, fingerprint, path = image.image_id, image.rootfs_identity_sha256, image.rootfs
        self.assertFalse(store.collect_image(image_id, is_referenced=lambda _: True))
        # Frontend restart + offline registry: local signed receipt is sufficient.
        store = EnvironmentRootfsStore(self.root / "store", self.registry, backend, runner=Runner(), referenced=lambda _: False)
        self.client.manifests.clear()
        with store.mounted_rootfs_lease(image_id, rootfs_identity_sha256=fingerprint) as resumed:
            self.assertEqual(path, resumed)
        metadata = {"schema": 2, "backend_abi": HOST_EROFS_ABI, "environment": manifest.to_dict(),
                    "rootfs_identity_sha256": fingerprint, "lowerdir": str(path)}
        self.assertEqual(OverlayRootfsManager._decode_environment(metadata, fingerprint), manifest)
        self.assertTrue(store.collect_image(image_id, is_referenced=lambda _: False))


if __name__ == "__main__":
    unittest.main()
