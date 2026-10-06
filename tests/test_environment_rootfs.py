import json
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

# Import the module, not the TestCase: discovery would rerun it here.
from tests import test_environment_artifact as artifact_fixtures
from ucloud_sandboxes.environment_artifact import (ENVIRONMENT_ANNOTATION, OCI_IMAGE,
    attach_environment_to_image, canonical_bytes, content_digest, load_environment, publish_environment)
from ucloud_sandboxes.environment_manifest import EnvironmentManifest, HOST_EROFS_ABI
from ucloud_sandboxes.environment_rootfs import EnvironmentImageRuntime, EnvironmentRootfsStore
from ucloud_sandboxes.images import ImageManager, ImageStore
from ucloud_sandboxes.image_rootfs import OverlayRootfsManager
from ucloud_sandboxes.sandbox import SandboxSpec, sandbox_spec_fingerprint


class EnvironmentRootfsTests(artifact_fixtures.EnvironmentArtifactTests):
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

    def test_images_sharing_a_filesystem_share_its_composition_but_keep_their_configs(self):
        # Two task images can differ only in config (Env, Cmd, WORKDIR, USER):
        # two signed roots over one component list.
        manifest, refs, source = EnvironmentManifest(self.digest), {}, self.component.source_image
        for name in ("a", "b"):
            root = publish_environment(self.registry, source_image=source, environment=manifest,
                image_config={"Cmd": [f"/bin/{name}"]}, signing_key=self.key, tag=f"root-{name}")
            self.client.manifests[name] = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE,
                "config": {"digest": source}, "layers": []})
            refs[name] = "localhost:5000/environments:" + name + "@" + attach_environment_to_image(
                self.registry, image_repository="environments", image_reference=name, environment_digest=root)
        self.client.base_url = "http://localhost:5000"
        mounts, mount_commands = set(), []
        def run(command, **kwargs):
            if command[0] == "mount":
                mount_commands.append(command)
                mounts.add(Path(command[-1]))
            return SimpleNamespace(returncode=int(command[0] == "mountpoint" and Path(command[-1]) not in mounts),
                                   stdout="", stderr="")
        backend = SimpleNamespace(ensure=lambda digest: self.root / "components" / digest[7:], drop=lambda digest: True)
        store = EnvironmentRootfsStore(self.root / "store", self.registry, backend,
                                       runner=SimpleNamespace(run=run), referenced=lambda _: False)
        with store.operation_lease(refs["a"]) as first, store.operation_lease(refs["b"]) as second:
            self.assertEqual((first.image_id, first.rootfs), (second.image_id, second.rootfs))
            self.assertEqual((first.image_config.command, second.image_config.command), (("/bin/a",), ("/bin/b",)))
        self.assertEqual(len(mount_commands), 1)
        # The node daemon leases the sibling by its resolution: the shared
        # composition's receipt names the other root.
        resolution = store.materialize_resolution(refs["b"])
        receipt = json.loads((store.images / first.image_id[7:] / "environment.json").read_text())
        self.assertEqual(resolution["source"], refs["b"])
        self.assertEqual(receipt["source"], refs["a"])
        self.assertNotEqual(resolution["root"], receipt["root"])
        self.assertEqual(resolution["environment"]["image_config"]["Cmd"], ["/bin/b"])
        self.assertEqual(len(mount_commands), 1)

    def test_a_dispatched_root_pins_the_image_and_survives_its_release(self):
        # Chunk store M2: the gateway dispatches the root; the worker binds it
        # to the image while the manifest exists, and uses it alone after.
        manifest, source = EnvironmentManifest(self.digest), self.component.source_image
        annotated_root = publish_environment(self.registry, source_image=source, environment=manifest,
            image_config={"Cmd": ["/bin/old"]}, signing_key=self.key, tag="root-old")
        new_root = publish_environment(self.registry, source_image=source, environment=manifest,
            image_config={"Cmd": ["/bin/new"]}, signing_key=self.key, tag="root-new")
        other = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE,
                                 "config": {"digest": "sha256:" + "7" * 64}, "layers": []})
        self.client.manifests[content_digest(other)] = other
        self.client.manifests["task"] = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE,
                                                         "config": {"digest": source}, "layers": []})
        digest = attach_environment_to_image(self.registry, image_repository="environments", image_reference="task",
                                             environment_digest=annotated_root)
        self.client.base_url = "http://localhost:5000"
        ref = "localhost:5000/environments:task@" + digest
        run = lambda command, **_: SimpleNamespace(returncode=0, stdout="", stderr="")  # noqa: E731
        backend = SimpleNamespace(ensure=lambda digest: self.root / "components" / digest[7:], drop=lambda digest: True)

        def store():
            return EnvironmentRootfsStore(self.root / "store", self.registry, backend,
                                          runner=SimpleNamespace(run=run), referenced=lambda _: False)
        with store().operation_lease(ref) as image:
            self.assertEqual(image.image_config.command, ("/bin/old",))  # No root: the annotation.
        with store().operation_lease(ref, new_root) as image:
            self.assertEqual(image.image_config.command, ("/bin/new",))
        with self.assertRaisesRegex(ValueError, "another OCI image"):  # A root binds to its image.
            with store().operation_lease("localhost:5000/environments:other@" + content_digest(other), new_root):
                pass
        for key in [key for key in self.client.manifests if key in (digest, "task")]:
            del self.client.manifests[key]  # Released: the manifest is gone.
        with store().operation_lease(ref, new_root) as image:
            self.assertEqual(image.image_config.command, ("/bin/new",))
        # The gateway's pull (the attach) carries the root too; without it the manifest is needed.
        manager = ImageManager(ImageStore(self.root / "image-api.sqlite"), EnvironmentImageRuntime(store()))
        self.assertEqual(manager.pull(ref, environment_root=new_root)[0].manifest_digest, digest)
        with self.assertRaisesRegex(ValueError, "MANIFEST_UNKNOWN"):
            manager.pull(ref)

    def test_the_spec_field_is_optional_and_keeps_old_fingerprints(self):
        raw = {"id": "s", "image": "localhost:5000/environments:task@sha256:" + "1" * 64, "cpus": 1, "memory_mb": 512}
        plain = SandboxSpec.from_dict(raw)
        self.assertNotIn("environment_root", plain.to_dict())
        self.assertEqual(sandbox_spec_fingerprint(SandboxSpec.from_dict(plain.to_dict())),
                         sandbox_spec_fingerprint(plain))
        pinned = SandboxSpec.from_dict({**raw, "environment_root": "sha256:" + "2" * 64})
        pinned.validate()
        self.assertEqual(SandboxSpec.from_dict(pinned.to_dict()), pinned)
        self.assertNotEqual(sandbox_spec_fingerprint(pinned), sandbox_spec_fingerprint(plain))
        with self.assertRaisesRegex(ValueError, "sha256 digest"):
            SandboxSpec.from_dict({**raw, "environment_root": "latest"}).validate()


if __name__ == "__main__":
    unittest.main()
