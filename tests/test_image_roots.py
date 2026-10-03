"""Chunk store M2 on the gateway: the image_roots table and journal, the
resolver's dispatched root, and placement's capability requirement."""
import json
import sqlite3
from types import SimpleNamespace
import unittest

# Import the module, not the TestCase: discovery would rerun it here.
from tests import test_environment_artifact as artifact_fixtures
from tests.harness import LocalFleet
from ucloud_sandboxes.capabilities import ENVIRONMENT_RAFS_CAPABILITY, ENVIRONMENT_ROOT_CAPABILITY
from ucloud_sandboxes.chunk_migrate import inventory
from ucloud_sandboxes.environment_artifact import OCI_IMAGE, attach_environment_to_image, canonical_bytes, publish_environment
from ucloud_sandboxes.environment_dependencies import EnvironmentDependencyResolver
from ucloud_sandboxes.environment_manifest import EnvironmentManifest
from ucloud_sandboxes.gateway.image_roots import ImageRootsStore, retention_view
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
        # Retention keeps converted, switched and released roots, and stops counting
        # a dispatched image's annotation; it needs no table to exist.
        store.record_converted("managed/c", D["5"], config_digest=D["2"], old_root=D["6"], new_root=D["7"],
                               wave="1", build_input=True)
        self.assertEqual(retention_view(self.root / "images.sqlite"), ({D["4"], D["7"]}, {("managed/a", D["1"])}))
        self.assertEqual(retention_view(self.root / "elsewhere" / "images.sqlite"), (set(), set()))

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


class InventoryTests(artifact_fixtures.EnvironmentArtifactTests):
    def test_inventory_lists_roots_build_inputs_families_and_releasable_bytes(self):
        manifest, source = EnvironmentManifest(self.digest), self.component.source_image
        root = publish_environment(self.registry, source_image=source, environment=manifest, image_config={},
                                   signing_key=self.key, tag="root")
        self.client.manifests["image-a"] = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE,
                                                            "config": {"digest": source}, "layers": []})
        digests = {"a": attach_environment_to_image(self.registry, image_repository="managed/a",
                                                    image_reference="image-a", environment_digest=root),
                   "b": D["2"], "c": D["3"]}
        layers = {"a": [(D["5"], 100), (D["6"], 7)], "b": [(D["7"], 50)], "c": [(D["6"], 7)]}
        for name in "bc":  # Images without an environment annotation.
            self.client.manifests[digests[name]] = canonical_bytes({"schemaVersion": 2, "mediaType": OCI_IMAGE,
                                                                    "config": {"digest": D["8"]}, "layers": []})

        class Registry:
            def catalog(self):
                return ["environments", "managed/a", "managed/b", "managed/c"]

            def tags(self, repository):
                return ["latest", "ucloud-digest-x"] if repository == "managed/a" else ["latest"]

            def manifest_digest(self, repository, tag):
                return digests[repository[-1]]

            def manifest_layers(self, repository, digest):
                items = [SimpleNamespace(digest=d, size=s) for d, s in layers[repository[-1]]]
                return SimpleNamespace(layers=items, total_size=sum(item.size for item in items))
        catalog = self.root / "prepared-images.sqlite3"
        db = sqlite3.connect(catalog)
        db.execute("CREATE TABLE prepared_sources (source TEXT, preparation TEXT, reference TEXT)")
        db.execute("CREATE TABLE prepared_foundations (key TEXT, source TEXT, family TEXT, base TEXT, payload TEXT)")
        db.execute("CREATE TABLE prepared_decisions (identity TEXT, payload TEXT)")
        db.execute("INSERT INTO prepared_sources VALUES ('x', 'source', ?)", ("r:5000/managed/c:latest@" + D["3"],))
        db.commit()
        db.close()
        selection = self.root / "selection.json"
        selection.write_text(json.dumps({"images": [
            {"family": "SWE-smith", "image": "x", "prepared_reference": "r:5000/managed/a:latest@" + digests["a"],
             "upstream_rows": 3}]}))
        summary = inventory(Registry(), self.registry, prefix="managed/", catalog_file=catalog, selection=selection,
                            out_rows=self.root / "rows.jsonl", out_summary=self.root / "summary.json")
        rows = {row["repository"]: row for row in map(json.loads, (self.root / "rows.jsonl").read_text().splitlines())}
        self.assertEqual(rows["managed/a"]["tags"], ["latest", "ucloud-digest-x"])  # Aliases of one digest.
        self.assertEqual((rows["managed/a"]["environment_root"], rows["managed/a"]["components"]), (root, [self.digest]))
        self.assertEqual((rows["managed/a"]["family"], rows["managed/a"]["task_rows"]), ("SWE-smith", 3))
        self.assertIsNone(rows["managed/b"]["environment_root"])
        self.assertEqual([row["build_input"] for row in rows.values()], [False, False, True])
        self.assertEqual((summary["images"], summary["environment_images"], summary["build_inputs"]), (3, 1, 1))
        self.assertEqual((summary["unique_oci_bytes"], summary["releasable_oci_bytes"]), (157, 150))  # D6 is kept.
        self.assertEqual(summary["families"]["SWE-smith"]["releasable_oci_bytes"], 100)
        self.assertEqual(summary["errors"], {})


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
