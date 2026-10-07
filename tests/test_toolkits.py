"""Toolkit layers (docs/toolkit-layers.md): the spec field, and the gateway's
composition of an image root with toolkit components."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from types import SimpleNamespace

from tests.harness import LocalFleet
from tests.test_environment_artifact import MemoryRegistry
from ucloud_sandboxes.capabilities import ENVIRONMENT_RAFS_CAPABILITY, ENVIRONMENT_ROOT_CAPABILITY
from ucloud_sandboxes.environment_artifact import (CHUNK_BYTES, EnvironmentArtifactRegistry, content_digest,
                                                   load_environment, publish_environment, sign_component,
                                                   sign_rafs_component)
from ucloud_sandboxes.environment_manifest import EnvironmentManifest
from ucloud_sandboxes.gateway.toolkits import ToolkitComposer, ToolkitError, ToolkitStore
from ucloud_sandboxes.sandbox import SandboxSpec, sandbox_spec_fingerprint

TEST_TIER = "contract"
PINNED = "vf-harness@sha256:" + "d" * 64


def spec(**values):
    return SandboxSpec(id="box", image="registry/image@sha256:" + "a" * 64, memory_mb=512, cpus=1, disk_mb=1024,
                       **values)


class ToolkitSpecTests(unittest.TestCase):
    def test_absent_toolkits_keep_every_spec_and_fingerprint_unchanged(self):
        plain = spec()
        self.assertNotIn("toolkits", plain.to_dict())
        self.assertEqual(SandboxSpec.from_dict({**plain.to_dict(), "toolkits": []}).to_dict(), plain.to_dict())
        self.assertNotEqual(sandbox_spec_fingerprint(spec(toolkits=(PINNED,))), sandbox_spec_fingerprint(plain))

    def test_requests_name_tags_and_the_gateway_pins_digests(self):
        for refs in (("vf-harness:latest",), (PINNED,), ("a:1", "b:2", "c:3", "d:4")):
            requested = spec(toolkits=refs)
            requested.validate()
            self.assertEqual(SandboxSpec.from_dict(requested.to_dict()).toolkits, refs)

    def test_malformed_repeated_or_too_many_toolkits_are_refused(self):
        for refs, message in (
            (("vf-harness",), "name:tag or name@sha256"),
            (("Vf:1",), "name:tag or name@sha256"),
            (("x@sha256:abc",), "name:tag or name@sha256"),
            (("a:1", "a:2"), "once"),
            (("a:1", "b:1", "c:1", "d:1", "e:1"), "at most 4"),
        ):
            with self.subTest(refs=refs), self.assertRaisesRegex(ValueError, message):
                spec(toolkits=refs).validate()


CONFIG = {"Entrypoint": [], "Cmd": ["bash"], "Env": ["PATH=/usr/bin"], "WorkingDir": "/", "User": ""}


class ToolkitCompositionTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.key = Ed25519PrivateKey.generate()
        public = self.key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        self.registry = EnvironmentArtifactRegistry(MemoryRegistry(), "environments", {content_digest(public): public})
        self.store = ToolkitStore(self.root / "toolkits.sqlite3")
        self.composer = ToolkitComposer(self.registry, self.store, self.key)
        self.toolkit_root, self.toolkit_component = self.erofs_root("toolkit", "9")
        self.store.register("vf-harness", "v1", self.toolkit_root)

    def erofs_root(self, name, fill):
        image = self.root / f"{name}.erofs"
        image.write_bytes(fill.encode() * (CHUNK_BYTES + 4096))
        component = sign_component(image, source_image="sha256:" + fill * 64, signing_key=self.key)
        digest = self.registry.publish(image, component, tag=f"{name}-component")
        root = publish_environment(self.registry, source_image="sha256:" + fill * 64,
                                   environment=EnvironmentManifest(digest), image_config=CONFIG,
                                   signing_key=self.key, tag=f"{name}-root")
        return root, digest

    def rafs_root(self):
        layers = ["sha256:" + "1" * 64, "sha256:" + "2" * 64]
        component = sign_rafs_component(source_image="sha256:" + "c" * 64, source_layers=layers,
                                        bootstrap={"digest": "sha256:" + "b" * 64, "size": 8192},
                                        chunk_map={"digest": "sha256:" + "d" * 64, "size": 44},
                                        device_size=4096 * 300, layout="image", signing_key=self.key)
        digest = self.registry.publish_rafs(component, tag="rafs-component")
        root = publish_environment(self.registry, source_image="sha256:" + "c" * 64,
                                   environment=EnvironmentManifest(digest), image_config=CONFIG,
                                   signing_key=self.key, tag="rafs-root", source_diff_ids=layers)
        return root, digest

    def test_a_rafs_image_gets_the_toolkit_on_top_in_one_signed_root(self):
        image_root, image_component = self.rafs_root()
        pinned = self.composer.pin(["vf-harness:v1"])
        self.assertEqual(pinned, (f"vf-harness@{self.toolkit_root}",))
        root = self.composer.compose(image_root, pinned)
        composed = load_environment(self.registry, root)  # Authenticated against the trusted key.
        self.assertEqual(composed.components, (image_component, self.toolkit_component))
        self.assertEqual((composed.source_image, composed.image_config),
                         ("sha256:" + "c" * 64, load_environment(self.registry, image_root).image_config))
        # Cached, and deterministic: a fresh gateway composes the same root.
        self.assertEqual(self.composer.compose(image_root, pinned), root)
        fresh = ToolkitComposer(self.registry, ToolkitStore(self.root / "other.sqlite3"), self.key)
        self.assertEqual(fresh.compose(image_root, pinned), root)
        self.assertIn(root, self.store.live_roots())
        self.assertIn(self.toolkit_root, self.store.live_roots())

    def test_a_whole_image_erofs_image_composes_too(self):
        image_root, image_component = self.erofs_root("image", "5")
        root = self.composer.compose(image_root, self.composer.pin(["vf-harness:v1"]))
        self.assertEqual(load_environment(self.registry, root).components, (image_component, self.toolkit_component))

    def test_unknown_tags_foreign_roots_and_composite_toolkits_are_refused(self):
        with self.assertRaisesRegex(ToolkitError, "not registered"):
            self.composer.pin(["vf-harness:v2"])
        other_root, _ = self.erofs_root("other", "7")
        with self.assertRaisesRegex(ToolkitError, "not a registered root"):
            self.composer.pin([f"vf-harness@{other_root}"])
        # A toolkit must be exactly one whole-image component.
        image_root, _ = self.erofs_root("image", "5")
        stacked = publish_environment(self.registry, source_image="sha256:" + "9" * 64,
                                      environment=EnvironmentManifest(self.toolkit_component,
                                                                      toolkits=(self.toolkit_component,)),
                                      image_config=CONFIG, signing_key=self.key, tag="stacked")
        self.store.register("stacked", "v1", stacked)
        with self.assertRaisesRegex(ToolkitError, "one whole-image component"):
            self.composer.compose(image_root, self.composer.pin(["stacked:v1"]))
        # A later tag moves; a pinned earlier root stays usable.
        self.store.register("vf-harness", "v1", other_root)
        self.assertEqual(self.composer.pin([f"vf-harness@{self.toolkit_root}"]),
                         (f"vf-harness@{self.toolkit_root}",))


class GatewayToolkitCreateTests(unittest.TestCase):
    SPEC = {"cpus": 1, "memory_mb": 256, "disk_mb": 1024, "network": "none"}
    IMAGE_ROOT, COMPOSED, TOOLKIT_ROOT = ("sha256:" + "5" * 64, "sha256:" + "6" * 64, "sha256:" + "9" * 64)

    def test_a_create_dispatches_the_composed_root_and_keeps_its_pins_on_retry(self):
        composer = SimpleNamespace(calls=[])

        def pin(refs):
            if any(ref.startswith("missing") for ref in refs):
                raise ToolkitError("toolkit missing:v1 is not registered")
            return tuple(f"{ref.split(':')[0]}@{self.TOOLKIT_ROOT}" for ref in refs)

        def compose(image_root, pinned):
            composer.calls.append((image_root, pinned))
            return self.COMPOSED

        composer.pin, composer.compose = pin, compose
        with LocalFleet(nodes=1) as fleet:
            node = fleet.nodes[0]
            node.server.RequestHandlerClass.__bases__[0].capabilities += (
                ENVIRONMENT_ROOT_CAPABILITY, ENVIRONMENT_RAFS_CAPABILITY)
            runtime = node.server.RequestHandlerClass.image_manager.runtime
            runtime.pulls_environment_roots, pull = True, runtime.pull
            runtime.pull = lambda image, environment_root=None: pull(image)
            fleet.catalog.add("harness/base:1", {"etc/hostname": "task\n"})
            fleet.heartbeat()
            handler = fleet.gateway.RequestHandlerClass
            handler.services.registry_refs.dependency_resolver = SimpleNamespace(
                root=lambda image: self.IMAGE_ROOT if image.startswith("harness/base:1") else None)
            payload = {"id": "tk", "image": "harness/base:1", "toolkits": ["vf-harness:v1"], **self.SPEC}

            unavailable = fleet.request("POST", "/v1/sandboxes", token="sandbox", payload=payload)
            self.assertEqual((unavailable.status, unavailable.json()["error_code"]), (400, "toolkits_unavailable"))
            handler.toolkit_composer = composer
            refused = fleet.request("POST", "/v1/sandboxes", token="sandbox",
                                    payload={**payload, "id": "tk-missing", "toolkits": ["missing:v1"]})
            self.assertEqual((refused.status, refused.json()["error_code"]), (400, "toolkit_refused"))
            created = fleet.request("POST", "/v1/sandboxes", token="sandbox", payload=payload)
            self.assertEqual(created.status, 201, created.body)
            pinned = [f"vf-harness@{self.TOOLKIT_ROOT}"]
            route = fleet.route("tk").spec
            self.assertEqual((route["toolkits"], route["environment_root"]), (pinned, self.COMPOSED))
            self.assertEqual(composer.calls, [(self.IMAGE_ROOT, tuple(pinned))])
            self.assertEqual(node.registration("tk").spec.environment_root, self.COMPOSED)
            # A retry after the tag moved keeps the route's pins: the same sandbox, not a conflict.
            composer.pin = lambda refs: tuple(f"{ref.split(':')[0]}@sha256:{'8' * 64}" for ref in refs)
            again = fleet.request("POST", "/v1/sandboxes", token="sandbox", payload=payload)
            self.assertIn(again.status, (200, 201), again.body)
            self.assertEqual(fleet.route("tk").spec["toolkits"], pinned)


if __name__ == "__main__":
    unittest.main()
