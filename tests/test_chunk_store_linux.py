"""§3 step 7 for real: mount converted images through the worker's own RAFS
device (NBD + kernel EROFS) and compare the whole tree with the OCI layers.

Needs root, nydus-image v2.4.5, free /dev/nbd* devices and EROFS device-table
support (Linux 5.16+); otherwise skipped.
"""
import os
import platform
from pathlib import Path
import shutil
import unittest

from tests.chunk_store_support import NYDUS, REPOSITORY, ChunkStoreFixture, sample_images
from ucloud_sandboxes.chunk_convert import mount_verifier

TEST_TIER = "linux"
BINARY = NYDUS or shutil.which("nydus-image") or ""
DEVICES = sorted(str(path) for path in Path("/dev").glob("nbd[0-9]*") if path.name[3:].isdigit())[-8:]
KERNEL = tuple(int(part) for part in platform.release().split("-")[0].split(".")[:2])


@unittest.skipUnless(os.geteuid() == 0 and BINARY and DEVICES and KERNEL >= (5, 16),
                     "needs root, nydus-image, NBD devices and Linux 5.16+ EROFS")
class MountedVerificationTests(unittest.TestCase):
    def test_both_granularities_mount_equal_to_their_oci_layers(self):
        for layout in ("image", "layer"):
            with self.subTest(layout=layout):
                store = ChunkStoreFixture(self, layout=layout, nydus=BINARY)
                sample_images(store.client)
                store.converter.verifier = mount_verifier(devices=DEVICES, trusted_keys=store.trusted,
                                                          work_root=store.root)
                for tag in ("a", "b"):
                    self.assertTrue(store.converter.convert(REPOSITORY, tag)["root"])


if __name__ == "__main__":
    unittest.main()
