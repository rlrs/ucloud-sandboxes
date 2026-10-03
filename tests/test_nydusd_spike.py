"""The nydusd spike's store side: blob-toc conversions keep nydus's chunk bytes
and blob tails, and the store node rebuilds every blob exactly from packs
(docs/benchmarks/nydusd-spike-2026-10-03)."""
import os
from pathlib import Path
import threading
import unittest
from tempfile import TemporaryDirectory

import shutil

from tests.chunk_store_support import NYDUS, REPOSITORY, ChunkStoreFixture, sample_images
from ucloud_sandboxes.chunk_index import http_range, http_request
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

    def test_virtual_blobs_need_the_read_token_and_a_known_blob(self):
        component, blob = self.blobs_kept()[0]
        with self.assertRaises(RegistryRequestError) as caught:
            self.get(component, blob, 0, 10, token="x" * 32)
        self.assertEqual(caught.exception.status_code, 401)
        with self.assertRaises(RegistryRequestError):
            self.get(component, "0" * 64, 0, 10)


@unittest.skipUnless(NYDUS or shutil.which("nydus-image"), "needs nydus-image v2.4.5 (UCLOUD_TEST_NYDUS_IMAGE)")
class RealVirtualBlobTests(VirtualBlobTests):
    """Real tails (chunk info, digests, TOC) and real cross-layer chunk bytes."""
    nydus = NYDUS or shutil.which("nydus-image")


if __name__ == "__main__":
    unittest.main()
