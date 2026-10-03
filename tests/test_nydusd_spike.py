"""The nydusd spike's store side: blob-toc conversions keep nydus's chunk bytes
and blob tails, and the store node rebuilds every blob exactly from packs
(docs/benchmarks/nydusd-spike-2026-10-03)."""
import hashlib
import os
from pathlib import Path
import threading
import unittest
from unittest import mock
from tempfile import TemporaryDirectory

import shutil

from tests.chunk_store_support import NYDUS, REPOSITORY, ChunkStoreFixture, sample_images
from ucloud_sandboxes.chunk_index import MissingChunks, http_range, http_request
from ucloud_sandboxes.chunk_store_node import ChunkStoreNode, ChunkStoreServer, ExtentCache, S3Source, VirtualBlobs
from ucloud_sandboxes.environment_artifact import load_environment
from ucloud_sandboxes.managed_registry import RegistryRequestError

TEST_TIER = "contract"
READ, WRITE = "r" * 32, "w" * 32


class VirtualBlobTests(unittest.TestCase):
    nydus = None  # The fake nydus-image.

    def setUp(self):
        kept = TemporaryDirectory()
        self.addCleanup(kept.cleanup)
        self.kept = Path(kept.name)
        os.environ["FAKE_NYDUS_KEEP"] = kept.name
        self.addCleanup(os.environ.pop, "FAKE_NYDUS_KEEP", None)
        nydus = None
        if self.nydus:  # Keep the real tool's blobs: run it, then copy its -D directory.
            nydus = self.kept / "nydus-image-keep"
            nydus.write_text(f'#!/bin/sh\n{self.nydus} "$@" || exit $?\nwhile [ $# -gt 0 ]; do\n'
                             f'  [ "$1" = -D ] && cp "$2"/* {self.kept}/ 2>/dev/null; shift\ndone\nexit 0\n')
            nydus.chmod(0o755)
        self.store = ChunkStoreFixture(self, nydus=nydus and str(nydus))
        self.store.converter.nydusd_blobs = True
        sample_images(self.store.client)
        self.roots = [self.store.converter.convert(REPOSITORY, tag)["root"] for tag in ("a", "b")]
        objects = self.store.objects
        node = ChunkStoreNode(ExtentCache(self.store.root / "node", 64 * 1024 ** 2),
                              S3Source(objects.presigner, "test/chunks", concurrency=4), extent_bytes=1024 ** 2,
                              warm_concurrency=2)
        self.addCleanup(node.close)
        self.server = ChunkStoreServer(("127.0.0.1", 0), node, read_token=READ, write_token=WRITE)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.server.blobs = VirtualBlobs(node, self.store.index.writer)

    def blobs_kept(self):
        """(component, blob id) of every converted blob; the repository part
        of the URL only names the asking image."""
        component = load_environment(self.store.registry, self.roots[0]).components[0][7:]
        return [(component, path.name) for path in sorted(self.kept.iterdir()) if len(path.name) == 64]

    def get(self, component, blob, start, length, token=READ):
        return http_range(f"{self.url}/v2/virtual/{component}/blobs/sha256:{blob}", start, length,
                          headers={"Authorization": "Bearer " + token})

    def test_every_blob_is_rebuilt_byte_for_byte(self):
        blobs = self.blobs_kept()
        self.assertGreaterEqual(len(blobs), 3)  # Base, top A, top B.
        for component, blob in blobs:
            expected = (self.kept / blob).read_bytes()
            with self.subTest(blob=blob):
                status, headers, _ = http_request("HEAD", f"{self.url}/v2/virtual/{component}/blobs/sha256:{blob}",
                                                  headers={"Authorization": "Bearer " + READ}, max_bytes=0)
                self.assertEqual((status, int(headers["Content-Length"])), (200, len(expected)))
                self.assertEqual(self.get(component, blob, 0, len(expected)), expected)
                for start, length in ((1, 7), (len(expected) // 3, len(expected) // 2), (len(expected) - 50, 50)):
                    self.assertEqual(self.get(component, blob, start, length), expected[start:start + length])

    def test_registration_with_a_store_node_stores_layouts_and_the_locator(self):
        # Gate run 3: computing locators per attach saturated the index. With
        # a store node, registration builds them once and reads need no index.
        service = self.store.index.service
        service.store_url = "http://store-node"
        with mock.patch.object(service.index, "locate", wraps=service.index.locate) as locate:
            for root in self.roots:
                environment = load_environment(self.store.registry, root)
                component = self.store.registry.load(environment.components[0])
                self.store.index.writer.register(environment.components[0], component.bootstrap["digest"],
                                                 component.chunk_map)
            built = locate.call_count
            service._locators.clear()  # A restarted index serves the stored locator.
            locator = self.store.index.reader.locator(environment.components[0])
        self.assertGreater(built, 0)
        self.assertEqual(locate.call_count, built)  # Not recomputed.
        self.assertTrue(all(url.startswith("http://store-node/") for _, url in locator.packs))
        self.server.blobs = VirtualBlobs(self.server.blobs.node, None)  # No index at all.
        for component_hex, blob in self.blobs_kept():
            expected = (self.kept / blob).read_bytes()
            with self.subTest(blob=blob):
                self.assertEqual(self.get(component_hex, blob, 0, len(expected)), expected)

    def test_head_sizes_a_blob_larger_than_one_response(self):
        component, blob = max(self.blobs_kept(), key=lambda item: (self.kept / item[1]).stat().st_size)
        with mock.patch("ucloud_sandboxes.chunk_store_node.MAX_RESPONSE_BYTES", 4096):
            status, headers, _ = http_request("HEAD", f"{self.url}/v2/virtual/{component}/blobs/sha256:{blob}",
                                              headers={"Authorization": "Bearer " + READ}, max_bytes=0)
        self.assertEqual((status, int(headers["Content-Length"])), (200, (self.kept / blob).stat().st_size))

    def test_virtual_blobs_need_the_read_token_and_a_known_blob(self):
        component, blob = self.blobs_kept()[0]
        with self.assertRaises(RegistryRequestError) as caught:
            self.get(component, blob, 0, 10, token="x" * 32)
        self.assertEqual(caught.exception.status_code, 401)
        with self.assertRaises(RegistryRequestError):
            self.get(component, "0" * 64, 0, 10)


@unittest.skipUnless(NYDUS or shutil.which("nydus-image"), "needs nydus-image v2.4.5 (UCLOUD_TEST_NYDUS_IMAGE)")
class TailLivenessTests(unittest.TestCase):
    def test_chunks_only_a_blob_tail_names_are_live_with_the_root(self):
        # Image a whites out the base layer's etc/gone: its chunk is in the
        # base blob, which nydusd reads across, but real merges leave it out
        # of a's chunk map (the fake never shadows).
        store = ChunkStoreFixture(self, nydus=NYDUS or shutil.which("nydus-image"))
        store.converter.nydusd_blobs = True
        sample_images(store.client)
        component = store.registry.load(load_environment(
            store.registry, store.converter.convert(REPOSITORY, "a")["root"]).components[0])
        gone, service = hashlib.sha256(b"x" * 5000).digest(), store.index.service
        self.assertNotIn(gone, service.chunk_map(component.chunk_map["digest"]).ids)
        self.assertIn(gone, [chunk for _, ids in service._blob_tails(component.bootstrap["digest"]) for chunk in ids])
        store.index.index._writer.execute("UPDATE chunks SET condemned = 1 WHERE id = ?", (gone,))
        with self.assertRaises(MissingChunks):  # GC must keep it while the root lives.
            service.register({"component": "sha256:" + "c" * 64, "bootstrap": component.bootstrap["digest"],
                              "chunk_map": component.chunk_map})


class NydusdDeviceTests(unittest.TestCase):
    def test_a_refused_device_raises_its_own_error(self):
        """Attach skips leased devices on EBUSY: a refusal must keep its type."""
        from types import SimpleNamespace
        from ucloud_sandboxes.environment_nydusd import NydusdDevice
        with TemporaryDirectory() as directory:
            path = Path(directory, "not-a-device")
            path.write_bytes(b"")
            image = SimpleNamespace(authenticate=lambda keys: None, bootstrap=SimpleNamespace(path=path))
            with self.assertRaisesRegex(ValueError, "real Linux NBD device"):
                NydusdDevice(path, image, None, trusted_keys={})

    def test_a_shared_cache_keeps_a_blob_until_its_last_image_detaches(self):
        from ucloud_sandboxes.environment_nydusd import NydusdFactory
        with TemporaryDirectory() as directory:
            binary = Path(directory, "nydusd-bin")
            binary.write_bytes(b"pinned")
            with self.assertRaisesRegex(ValueError, "pinned sha256"):
                NydusdFactory(str(binary), "0" * 64, "http://store", "token", Path(directory, "nydusd"))
            factory = NydusdFactory(str(binary), hashlib.sha256(b"pinned").hexdigest(), "http://store", "token",
                                    Path(directory, "nydusd"))
            base, top = "a" * 64, "b" * 64
            for blob in (base, top):
                for suffix in (".blob.data", ".blob.meta"):
                    (factory.cache / (blob + suffix)).write_bytes(b"x")
            factory.acquire((base, top))
            factory.acquire((base,))
            factory.release((base, top))
            self.assertEqual(sorted(path.name[:1] for path in factory.cache.iterdir()), ["a", "a"])
            factory.release((base,))
            self.assertEqual(list(factory.cache.iterdir()), [])


@unittest.skipUnless(NYDUS or shutil.which("nydus-image"), "needs nydus-image v2.4.5 (UCLOUD_TEST_NYDUS_IMAGE)")
class RealVirtualBlobTests(VirtualBlobTests):
    """Real tails (chunk info, digests, TOC) and real cross-layer chunk bytes."""
    nydus = NYDUS or shutil.which("nydus-image")


if __name__ == "__main__":
    unittest.main()
