from __future__ import annotations

import hashlib
import json
import os
import unittest

from ucloud_sandboxes.managed_registry import RegistryClient


REGISTRY_URL = os.environ.get("UCLOUD_TEST_REGISTRY_URL", "").rstrip("/")


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


@unittest.skipUnless(
    REGISTRY_URL,
    "set UCLOUD_TEST_REGISTRY_URL to run the Distribution contract",
)
class RegistryDistributionContractTests(unittest.TestCase):
    def test_blob_manifest_listing_and_delete_contract(self) -> None:
        client = RegistryClient(REGISTRY_URL, timeout_seconds=10)
        repository = "ucloud-contract/image"
        tag = "round-trip"
        layer = b"ucloud registry contract layer\n"
        layer_digest = _digest(layer)
        config = json.dumps(
            {
                "architecture": "amd64",
                "config": {},
                "os": "linux",
                "rootfs": {"diff_ids": [layer_digest], "type": "layers"},
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        config_digest = _digest(config)

        for payload, digest in ((config, config_digest), (layer, layer_digest)):
            location = client.start_blob_upload(repository)
            location = client.upload_blob_chunk(location, payload)
            self.assertEqual(client.finish_blob_upload(location, digest), digest)
            self.assertTrue(client.blob_exists(repository, digest))
            self.assertEqual(client.blob_bytes(repository, digest), payload)

        manifest = json.dumps(
            {
                "config": {
                    "digest": config_digest,
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "size": len(config),
                },
                "layers": [
                    {
                        "digest": layer_digest,
                        "mediaType": "application/vnd.oci.image.layer.v1.tar",
                        "size": len(layer),
                    }
                ],
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "schemaVersion": 2,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        manifest_digest = client.put_manifest(repository, tag, manifest)

        self.assertIn(repository, client.catalog())
        self.assertIn(tag, client.tags(repository))
        self.assertEqual(client.manifest_digest(repository, tag), manifest_digest)
        layers = client.manifest_layers(repository, tag)
        self.assertEqual(layers.manifest_digest, manifest_digest)
        self.assertEqual(
            [(item.digest, item.size) for item in layers.layers],
            [(layer_digest, len(layer))],
        )

        client.delete_manifest(repository, manifest_digest)
        self.assertFalse(client.tag_exists(repository, tag))


if __name__ == "__main__":
    unittest.main()
