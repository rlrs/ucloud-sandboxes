"""The converter with the real nydus-image v2.4.5, and the unpack rollback.

Set UCLOUD_TEST_NYDUS_IMAGE (or put nydus-image on PATH); otherwise skipped.
"""
import base64
import io
from pathlib import Path
import shutil
import tarfile
import unittest

from tests.chunk_store_support import NYDUS, REPOSITORY, ChunkStoreFixture, sample_images
from ucloud_sandboxes.chunk_convert import compare_trees, expected_tree, unpack_environment, verify_regeneration
from ucloud_sandboxes.environment_artifact import load_environment
from ucloud_sandboxes.environment_cache import VerifiedEnvironmentCache
from ucloud_sandboxes.environment_rafs import load_rafs_image

TEST_TIER = "contract"
BINARY = NYDUS or shutil.which("nydus-image") or ""


@unittest.skipUnless(BINARY, "needs nydus-image v2.4.5 (UCLOUD_TEST_NYDUS_IMAGE)")
class RealConversionTests(unittest.TestCase):
    def convert(self, layout):
        store = ChunkStoreFixture(self, layout=layout, nydus=BINARY)
        images = sample_images(store.client)
        roots = [store.converter.convert(REPOSITORY, tag)["root"] for tag in ("a", "b")]
        return store, images, roots

    def test_converted_images_read_back_verified_through_the_device(self):
        for layout in ("image", "layer"):
            store, _, roots = self.convert(layout)
            cache = VerifiedEnvironmentCache(store.root / "cache", None)
            self.addCleanup(cache.close)
            for root in roots:
                for digest in load_environment(store.registry, root).components:
                    image = load_rafs_image(digest, store.registry.load(digest), store.index.reader)
                    data = cache.read(image, 0, image.image_size)  # Every chunk verifies.
                    self.assertEqual(data[:image.bootstrap.size], image.bootstrap.read(0, image.bootstrap.size))
            with self.subTest(layout=layout):
                self.assertEqual(len(load_environment(store.registry, roots[0]).components),
                                 1 if layout == "image" else 2)

    def test_unpack_regenerates_an_equal_oci_tree(self):
        store, _, roots = self.convert("image")
        result = unpack_environment(store.registry, store.index.reader, roots[0], repository=REPOSITORY,
                                    tag="rollback", work_root=store.root, nydus_image=BINARY)
        document, _ = store.client.manifest_document(REPOSITORY, "rollback")
        self.assertNotIn("annotations", document)
        (layer,) = document["layers"]
        regenerated = store.root / "regenerated.tar.gz"
        regenerated.write_bytes(store.client.blobs[layer["digest"]])
        originals = []
        manifest, _ = store.client.manifest_document(REPOSITORY, "a")
        for index, item in enumerate(manifest["layers"]):
            originals.append(store.root / f"original-{index}.tar.gz")
            originals[-1].write_bytes(store.client.blobs[item["digest"]])
        self.assertEqual(compare_trees(expected_tree(originals), expected_tree([regenerated])), [])
        with tarfile.open(fileobj=io.BytesIO(store.client.blobs[layer["digest"]])) as reader:
            self.assertNotIn("etc/gone", reader.getnames())
        self.assertEqual(result["root"], roots[0])
        self.assertTrue(Path(store.root).exists())

    def test_a_verified_receipt_is_reproduced_with_the_original_config(self):
        store, _, roots = self.convert("image")
        receipt = verify_regeneration(store.registry, store.index.reader, roots[0], store.client, REPOSITORY, "a",
                                      work_root=store.root, nydus_image=BINARY)
        self.assertEqual((receipt["verified"], receipt["differences"]), (True, []))
        result = unpack_environment(store.registry, store.index.reader, roots[0], repository=REPOSITORY,
                                    tag="copy", work_root=store.root, nydus_image=BINARY, diff_id=receipt["diff_id"],
                                    image_config=base64.b64decode(receipt["config"]))
        self.assertEqual(result["diff_id"], receipt["diff_id"])  # Deterministic: volume-free builds rely on it.

    def test_an_extended_root_unpacks_to_its_parent_with_the_layer_applied(self):
        from tests.test_chunk_convert import commit_layer
        store, _, roots = self.convert("image")
        tar, diff_id = commit_layer(store.root)
        config = {"Entrypoint": [], "Cmd": ["sh"], "Env": ["PATH=/bin"], "WorkingDir": "/", "User": ""}
        child = store.converter.extend(roots[0], tar, diff_id, image_config=config, repository=REPOSITORY)["root"]
        cache = VerifiedEnvironmentCache(store.root / "cache", None)
        self.addCleanup(cache.close)
        digest = load_environment(store.registry, child).environment.base
        image = load_rafs_image(digest, store.registry.load(digest), store.index.reader)
        cache.read(image, 0, image.image_size)  # Parent and new chunks verify.
        unpack_environment(store.registry, store.index.reader, child, repository=REPOSITORY, tag="child",
                           work_root=store.root, nydus_image=BINARY)
        document, _ = store.client.manifest_document(REPOSITORY, "child")
        regenerated = store.root / "child.tar.gz"
        regenerated.write_bytes(store.client.blobs[document["layers"][0]["digest"]])
        originals = []
        manifest, _ = store.client.manifest_document(REPOSITORY, "a")
        for index, item in enumerate(manifest["layers"]):
            originals.append(store.root / f"original-{index}.tar.gz")
            originals[-1].write_bytes(store.client.blobs[item["digest"]])
        self.assertEqual(compare_trees(expected_tree([*originals, tar]), expected_tree([regenerated])), [])
        with tarfile.open(regenerated) as reader:
            names = set(reader.getnames())
        self.assertIn("opt/fresh", names)
        self.assertFalse({"etc/hosts", "opt/a", "opt/a-only"} & names)  # Whiteout and opaque applied.
        self.assertIn("usr/lib/text", names)


if __name__ == "__main__":
    unittest.main()
