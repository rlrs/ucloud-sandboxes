import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

# Import the module, not the TestCase: discovery would rerun it here.
from tests import test_environment_artifact as artifact_fixtures
from ucloud_sandboxes.environment_artifact import (ENVIRONMENT_ANNOTATION, OCI_IMAGE,
    attach_environment_to_image, canonical_bytes, content_digest, load_environment, publish_environment)
from ucloud_sandboxes.environment_manifest import EnvironmentManifest, HOST_EROFS_ABI
from ucloud_sandboxes.environment_backend import NO_BLOCK_DEVICE
from ucloud_sandboxes.environment_rootfs import (EnvironmentDeviceCapacityError, EnvironmentImageRuntime,
    EnvironmentRootfsStore)
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


class IdleImageSweepTests(unittest.TestCase):
    """Idle compositions stay mounted; the sweep collects them LRU first, only over budget."""

    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.now, self.over_cache, self.dead = 10_000.0, False, set()
        self.mounts, self.dependents, self.referenced = set(), set(), set()
        self.attached, self.ensured, self.environments = set(), [], {}

    def store(self, devices=8, percent=50, pressure=True):
        backend = SimpleNamespace(ensure=self.ensure, drop=self.drop)
        if pressure:
            backend.pressure = lambda: {"active_components": len(self.attached), "over_cache_budget": self.over_cache}
        store = EnvironmentRootfsStore(self.root / "store", None, backend, runner=SimpleNamespace(run=self.execute),
                                       referenced=lambda path: path in self.dependents, block_devices=devices,
                                       device_budget_percent=percent, clock=lambda: self.now)
        store.is_referenced = lambda image_id: image_id in self.referenced
        store._load = lambda image_id: (self.environments[image_id], None)  # Signed receipts: other tests.
        self.devices = devices
        return store

    def execute(self, command, **_kwargs):
        if command[0] in ("mount", "env"):
            self.mounts.add(Path(command[-1]))
        elif command[0] == "umount":
            self.mounts.discard(Path(command[-1]))
        code = int(Path(command[-1]) not in self.mounts) if command[0] == "mountpoint" else 0
        return SimpleNamespace(returncode=code, stdout="", stderr="")

    def ensure(self, digest):
        self.ensured.append(digest)
        if digest in self.dead:
            raise RuntimeError("environment block export failed; fence and drain affected sandboxes")
        if digest not in self.attached and len(self.attached) >= self.devices:
            raise RuntimeError(NO_BLOCK_DEVICE)
        self.attached.add(digest)
        return self.root / "components" / digest[7:]

    def drop(self, digest):
        # The backend refuses a component another mounted composition stacks (EBUSY).
        if any(digest in environment.components and self.rootfs(image_id) in self.mounts
               for image_id, environment in self.environments.items()):
            return False
        self.attached.discard(digest)
        return True

    def rootfs(self, image_id):
        return self.root / "store" / "images" / image_id[7:] / "rootfs"

    def image(self, store, name, *components, used=0.0, mount=True):
        image_id = "sha256:" + name * 64
        environment = SimpleNamespace(components=tuple("sha256:" + item * 64 for item in components))
        self.environments[image_id] = environment
        self.rootfs(image_id).mkdir(parents=True)
        (self.rootfs(image_id).parent / "environment.json").write_text("{}")
        if mount:
            store._mount(image_id, environment)
        store.note_used(image_id) if used is None else store._last_used.__setitem__(image_id, used)
        return image_id

    def present(self, store):
        return {"sha256:" + path.name for path in store.images.iterdir()}

    def test_the_sweep_runs_only_over_the_device_budget_least_recently_used_first(self):
        store = self.store()  # 8 devices, budget 4.
        images = [self.image(store, name, str(index), used=100.0 * index) for index, name in enumerate("abcd", 1)]
        self.assertEqual(store.collect_idle(), ())  # 4 attached: within budget.
        oldest = self.image(store, "e", "5", "6", used=50.0)  # Two components: 6 attached.
        self.assertEqual(store.collect_idle(), (oldest,))  # Down to the budget, no further.
        newest = self.image(store, "f", "7", "8", used=500.0)
        self.assertEqual(store.collect_idle(), tuple(images[:2]))
        self.assertEqual(self.present(store), {*images[2:], newest})
        self.assertEqual(len(self.attached), 4)
        self.assertEqual(store.operation_snapshot()["environment_devices_in_use"], 4)

    def test_the_sweep_never_collects_a_referenced_leased_in_use_or_recent_image(self):
        store = self.store(percent=1)  # Budget 0: every idle image is over it.
        referenced, mounted, leased, recent, idle = (
            self.image(store, name, str(index), used=used)
            for index, (name, used) in enumerate(zip("abcde", (1.0, 2.0, 3.0, None, 4.0))))
        self.referenced.add(referenced)  # A registration: noded's creates commit one too.
        self.dependents.add(self.rootfs(mounted))  # A sandbox overlay stacks it.
        with store._lease(leased):  # A create or materialization holds it.
            self.assertEqual(store.collect_idle(), (idle,))
        self.assertEqual(store._last_used[referenced], self.now)  # Seen in use now.
        self.assertEqual(store.collect_idle(), (leased,))
        self.assertEqual(self.present(store), {referenced, mounted, recent})
        self.now += 60
        self.assertEqual(store.collect_idle(), (recent,))
        store.is_referenced = None  # No registry bound: no sweep.
        self.dependents.clear()
        self.assertEqual(store.collect_idle(), ())

    def test_a_component_another_composition_stacks_stays_attached(self):
        store = self.store(percent=1)
        first = self.image(store, "a", "0", "1", used=1.0)
        second = self.image(store, "b", "0", "2", used=None)
        self.assertEqual(store.collect_idle(), (first,))
        self.assertEqual(self.attached, {"sha256:" + "0" * 64, "sha256:" + "2" * 64})
        self.assertIn(self.rootfs(second), self.mounts)

    def test_a_device_cache_over_its_bytes_collects_idle_images_until_under(self):
        store = self.store()
        old, newer = self.image(store, "a", "1", used=1.0), self.image(store, "b", "2", used=2.0)
        self.over_cache = True
        original = self.drop

        def drop(digest):
            self.over_cache = False  # The last user of the blob detached.
            return original(digest)
        store.backend.drop = drop
        self.assertEqual(store.collect_idle(), (old,))
        self.assertEqual(self.present(store), {newer})

    def test_device_exhaustion_collects_idle_images_and_retries_once(self):
        store = self.store(devices=2, percent=75, pressure=False)  # An older backend: the mounted view.
        idle = self.image(store, "a", "1", "2", used=1.0)
        wanted = self.image(store, "b", "3", mount=False, used=None)
        self.assertIsNotNone(store._mount(wanted, self.environments[wanted]))
        self.assertEqual(self.present(store), {wanted})
        self.assertEqual(self.attached, {"sha256:" + "3" * 64})
        # Nothing collectable: the capacity error, after one attempt.
        store = self.store(devices=1, percent=100)
        self.referenced.add(wanted)
        self.ensured.clear()
        other = self.image(store, "c", "4", mount=False, used=None)
        with self.assertRaises(EnvironmentDeviceCapacityError):
            store._mount(other, self.environments[other])
        self.assertEqual(self.ensured, ["sha256:" + "4" * 64])

    def test_reconciliation_keeps_live_idle_images_and_collects_the_rest(self):
        store = self.store(percent=1)
        root, idle = self.image(store, "a", "1", used=1.0), self.image(store, "b", "2", used=2.0)
        dead, unmounted = self.image(store, "c", "3", used=3.0), self.image(store, "d", "4", mount=False)
        self.dead.add("sha256:" + "3" * 64)
        restarted = self.store(percent=1)
        self.assertEqual(restarted.reconcile_images((root,), is_referenced=lambda _: False),
                         {"collected": 2, "retained": 1, "idle": 1})
        self.assertEqual(self.present(restarted), {root, idle})
        self.assertEqual(restarted.operation_snapshot()["environment_devices_in_use"], 2)
        self.assertNotIn(dead, self.present(restarted))
        self.assertNotIn(unmounted, self.present(restarted))
        # After a restart the receipt's write time orders the sweep.
        later = self.image(restarted, "e", "5")
        del restarted._last_used[later]
        os.utime(self.rootfs(idle).parent / "environment.json", (1.0, 1.0))
        os.utime(self.rootfs(later).parent / "environment.json", (2.0, 2.0))
        self.referenced.add(root)
        self.assertEqual(restarted.collect_idle(), (idle, later))


if __name__ == "__main__":
    unittest.main()
