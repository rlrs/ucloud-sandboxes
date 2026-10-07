"""builder_format "rafs": a build's last step converts its image into the chunk store."""
from types import SimpleNamespace
import unittest

from tests.chunk_store_support import REPOSITORY, ChunkStoreFixture, fake_nydus, sample_images
from ucloud_sandboxes.chunk_convert import rafs_build_publisher
from ucloud_sandboxes.environment_artifact import RafsEnvironmentComponent, load_environment, load_image_environment
from ucloud_sandboxes.environment_builder import publication_metrics


class RafsBuildPublisherTests(unittest.TestCase):
    def test_a_build_ends_in_the_chunk_store_and_reuses_converted_layers(self):
        fixture = ChunkStoreFixture(self)
        sample_images(fixture.client)  # Tags a and b share their base layer.
        store = SimpleNamespace(object_store=lambda: fixture.index.store, nydus_image=fake_nydus(fixture.root),
                                mount_granularity="image", nydusd=None)
        publish = rafs_build_publisher(fixture.registry, store, fixture.index.writer, fixture.key,
                                       fixture.root / "builder-work")
        results = {}
        for tag in ("a", "b"):
            with publication_metrics() as metrics:
                digest = publish(SimpleNamespace(tag=f"10.0.0.1:5000/{REPOSITORY}:{tag}"))
            results[tag] = (digest, dict(metrics))

        for tag, (digest, metrics) in results.items():
            # The tag now names the annotated copy, as the EROFS publisher leaves it.
            self.assertEqual(fixture.client.manifest_document(REPOSITORY, tag)[0],
                             fixture.client.manifest_document(REPOSITORY, digest)[0])
            root, environment = load_image_environment(fixture.registry, REPOSITORY, digest)
            self.assertIs(type(fixture.registry.load(environment.environment.base)), RafsEnvironmentComponent)
            self.assertEqual(load_environment(fixture.registry, root).environment.base,
                             environment.environment.base)
            self.assertGreater(metrics["rafs_convert_ms"], 0)
        self.assertNotIn("rafs_layers_reused", results["a"][1])
        self.assertEqual(results["b"][1]["rafs_layers_reused"], 1)  # The shared base, converted once.
        self.assertEqual(results["b"][1]["rafs_layers_converted"], 1)  # Only b's own layer.


if __name__ == "__main__":
    unittest.main()


class RecipeReleaseTests(unittest.TestCase):
    """The gateway's per-image release: born in the chunk store, then OCI-released."""

    def test_a_chunk_store_image_gets_its_roots_row_and_its_manifest_released(self):
        from pathlib import Path
        from unittest.mock import patch
        from ucloud_sandboxes.control_plane import ControlPlaneHandler
        from ucloud_sandboxes.gateway.image_roots import ImageRootsStore
        fixture = ChunkStoreFixture(self)
        sample_images(fixture.client)
        store = SimpleNamespace(object_store=lambda: fixture.index.store, nydus_image=fake_nydus(fixture.root),
                                mount_granularity="image", nydusd=None)
        digest = rafs_build_publisher(fixture.registry, store, fixture.index.writer, fixture.key,
                                      fixture.root / "work")(SimpleNamespace(tag=f"10.0.0.1:5000/{REPOSITORY}:a"))
        roots = ImageRootsStore(Path(fixture.root) / "image-roots.sqlite3")
        handler = SimpleNamespace(
            services=SimpleNamespace(
                registry_refs=SimpleNamespace(dependency_resolver=SimpleNamespace(registry=fixture.registry,
                                                                                  image_roots=roots),
                                              usage_store=object()),
                images=SimpleNamespace(registry_url="http://10.0.0.1:5000")),
            prepared_image_catalog=SimpleNamespace(path=Path(fixture.root) / "prepared.sqlite3"), routing_store=None)
        calls = []

        def release_oci(roots_, client, usage, wave, *, keys, execute, **kwargs):
            calls.append((wave, keys, execute))
            for key in keys:
                roots_.mark_oci_released(*key, layer_bytes=1)
            return {"released": len(keys)}

        reference = f"10.0.0.1:5000/{REPOSITORY}:a@{digest}"
        with patch("ucloud_sandboxes.chunk_migrate.release_oci", release_oci):
            outcome = ControlPlaneHandler._release_recipe_images(handler, {"recipe-x": reference})
            again = ControlPlaneHandler._release_recipe_images(handler, {"recipe-x": reference})
        root, _ = load_image_environment(fixture.registry, REPOSITORY, digest)
        row = roots.get(REPOSITORY, digest)
        self.assertEqual(outcome, {"recipe-x": "released"})
        self.assertEqual(again, {"recipe-x": "released"})  # Idempotent.
        self.assertEqual((row["state"], row["old_root"], row["new_root"], row["wave"]), ("released", root, root, "recipe"))
        self.assertEqual(roots.released_digest(REPOSITORY, digest), digest)  # Creates resolve it without a manifest.
        self.assertEqual(calls[0], ("recipe", {(REPOSITORY, digest)}, True))
