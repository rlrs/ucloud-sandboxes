"""Chunk store M2 on the gateway: the image_roots table and journal, the
resolver's dispatched root, and placement's capability requirement."""
from types import SimpleNamespace
import unittest

# Import the module, not the TestCase: discovery would rerun it here.
from tests import test_environment_artifact as artifact_fixtures
from tests.harness import LocalFleet
from ucloud_sandboxes.capabilities import ENVIRONMENT_RAFS_CAPABILITY, ENVIRONMENT_ROOT_CAPABILITY
from ucloud_sandboxes.environment_artifact import OCI_IMAGE, attach_environment_to_image, canonical_bytes, publish_environment
from ucloud_sandboxes.environment_dependencies import EnvironmentDependencyResolver
from ucloud_sandboxes.environment_manifest import EnvironmentManifest
from ucloud_sandboxes.gateway.image_roots import ImageRootsStore, live_roots_for
from ucloud_sandboxes.gateway.placement import _sandbox_required_capabilities
from ucloud_sandboxes.sandbox import SandboxSpec

TEST_TIER = "contract"
D = {name: "sha256:" + name * 64 for name in "123456789"}


class ImageRootsTests(artifact_fixtures.EnvironmentArtifactTests):
    def store(self):
        return ImageRootsStore(self.root / "image-roots.sqlite3")

    def test_rows_move_converted_switched_released_and_journal_every_step(self):
        store = self.store()
        row = dict(config_digest=D["2"], old_root=D["3"], new_root=D["4"], wave="1", build_input=False)
        store.record_converted("managed/a", D["1"], **row)
        self.assertIsNone(store.dispatch_root("managed/a", D["1"]))  # Converted: the annotation decides.
        self.assertEqual(store.live_roots(), {D["4"]})
        store.transition("managed/a", D["1"], "switched")
        self.assertEqual(store.dispatch_root("managed/a", D["1"]), D["4"])
        with self.assertRaisesRegex(ValueError, "revert it first"):
            store.record_converted("managed/a", D["1"], **row)
        store.transition("managed/a", D["1"], "reverted")
        self.assertIsNone(store.dispatch_root("managed/a", D["1"]))
        self.assertEqual(store.live_roots(), set())
        store.transition("managed/a", D["1"], "switched")
        store.transition("managed/a", D["1"], "released")
        with self.assertRaisesRegex(ValueError, "cannot become"):
            store.transition("managed/a", D["1"], "reverted")
        self.assertEqual([entry[5] for entry in store.journal("managed/a")],
                         ["converted", "switched", "reverted", "switched", "released"])
        with self.assertRaises(KeyError):
            store.transition("managed/b", D["1"], "switched")
        self.assertEqual(ImageRootsStore(self.root / "image-roots.sqlite3").get("managed/a", D["1"])["state"],
                         "released")  # Reopening an existing file keeps it.
        # Retention keeps released, switched and converted roots, and needs no table to exist.
        self.assertEqual(live_roots_for(self.root / "images.sqlite"), {D["4"]})
        self.assertEqual(live_roots_for(self.root / "elsewhere" / "images.sqlite"), set())

    def test_the_resolver_dispatches_a_switched_root_and_its_closure(self):
        manifest, source = EnvironmentManifest(self.digest), self.component.source_image
        old = publish_environment(self.registry, source_image=source, environment=manifest,
                                  image_config={}, signing_key=self.key, tag="old")
        new = publish_environment(self.registry, source_image=source, environment=manifest,
                                  image_config={"Cmd": ["/bin/new"]}, signing_key=self.key, tag="new")
        self.client.manifests["image"] = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE,
                                                          "config": {"digest": source}, "layers": []})
        annotated = attach_environment_to_image(self.registry, image_repository="managed/a",
                                                image_reference="image", environment_digest=old)
        image = "registry.example/managed/a:latest@" + annotated
        roots = self.store()
        resolver = EnvironmentDependencyResolver(self.registry, image_roots=roots)
        self.assertEqual(resolver.root(image), old)
        roots.record_converted("managed/a", annotated, config_digest=source, old_root=old, new_root=new, wave="1",
                               build_input=False)
        self.assertEqual(resolver.root(image), old)
        roots.transition("managed/a", annotated, "switched")
        self.assertEqual(resolver.root(image), new)  # The cache never serves the closure from before.
        self.assertEqual({identity for *_, identity in resolver(image)}, {new, self.digest})
        roots.transition("managed/a", annotated, "reverted")
        self.assertEqual(resolver.root(image), old)

    def test_a_dispatched_root_requires_both_worker_capabilities(self):
        spec = SandboxSpec(id="s", image="r/a@" + D["1"], cpus=1, memory_mb=512)
        self.assertEqual(_sandbox_required_capabilities(spec.to_dict()), ())
        pinned = SandboxSpec(id="s", image="r/a@" + D["1"], cpus=1, memory_mb=512, environment_root=D["2"])
        self.assertEqual(_sandbox_required_capabilities(pinned.to_dict()),
                         (ENVIRONMENT_ROOT_CAPABILITY, ENVIRONMENT_RAFS_CAPABILITY))


class GatewayDispatchTests(unittest.TestCase):
    SPEC = {"cpus": 1, "memory_mb": 256, "disk_mb": 1024, "network": "none"}

    def test_clients_cannot_choose_a_root_and_only_capable_workers_take_one(self):
        with LocalFleet(nodes=1) as fleet:
            handler = fleet.gateway.RequestHandlerClass
            mapped = {"harness/base:1": D["7"]}
            handler.dispatch_environment_roots = True
            handler.services.registry_refs.dependency_resolver = SimpleNamespace(
                root=lambda image: next((root for name, root in mapped.items() if image.startswith(name)), None))
            chosen = fleet.request("POST", "/v1/sandboxes", token="sandbox", payload={
                "id": "chosen", "image": "harness/base:1", "environment_root": D["8"], **self.SPEC})
            self.assertEqual(chosen.status, 400, chosen.body)
            self.assertIn("set by the gateway", chosen.json()["error"])
            # The harness workers' Docker store honours no root: no capable worker.
            pinned = fleet.request("POST", "/v1/sandboxes", token="sandbox", payload={
                "id": "pinned", "image": "harness/base:1", **self.SPEC})
            self.assertEqual(pinned.status, 503, pinned.body)
            self.assertTrue(pinned.json()["retryable"])
            self.assertEqual(pinned.json()["error_code"], "no_ready_node", pinned.body)
            self.assertIsNone(fleet.route("pinned"))
            mapped.clear()  # No environment: nothing is pinned and nothing is required.
            plain = fleet.request("POST", "/v1/sandboxes", token="sandbox", payload={
                "id": "plain", "image": "harness/base:1", **self.SPEC})
            self.assertEqual(plain.status, 201, plain.body)
            self.assertNotIn("environment_root", fleet.route("plain").spec)


if __name__ == "__main__":
    unittest.main()
