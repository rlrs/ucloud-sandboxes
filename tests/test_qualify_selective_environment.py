import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from scripts.qualify_selective_environment import ReadOnlyClient, capture_registry
from ucloud_sandboxes.environment_artifact import content_digest, layer_chain_id, sign_layer_component


class SelectiveQualificationTests(unittest.TestCase):
    def test_client_blocks_every_unlisted_operation(self):
        client = ReadOnlyClient(SimpleNamespace(blob_bytes=lambda *_: b"payload", put_manifest=lambda: self.fail()))
        self.assertEqual(client.blob_bytes("repo", "digest"), b"payload")
        for name in ("put_manifest", "upload_blob_file", "delete_manifest", "request", "unknown_read"):
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                getattr(client, name)

    def test_capture_authenticates_and_rehashes_without_any_registry_calls(self):
        key = Ed25519PrivateKey.generate()
        public = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        registry = capture_registry(SimpleNamespace(), "environments", {content_digest(public): public})
        with TemporaryDirectory() as temporary:
            image = Path(temporary) / "component.erofs"
            image.write_bytes(b"x" * 4096)
            component = sign_layer_component(image, source_layers=[content_digest(b"layer")],
                parent=layer_chain_id([]), layer_format={"layout": 1, "mkfs": "qualification-test",
                    "compression": "lz4", "excludes": []}, signing_key=key)
            handle = registry.publish(image, component, tag="owned-test")
            self.assertIs(registry.load(handle), component)
            self.assertEqual(registry.publications[0]["component"]["image_digest"],
                             "sha256:" + hashlib.sha256(b"x" * 4096).hexdigest())
            self.assertTrue(registry.publications[0]["bytes_rehashed"])
            image.write_bytes(b"y" * 4096)
            with self.assertRaisesRegex(ValueError, "bytes differ"):
                registry.publish(image, component, tag="owned-test")
            self.assertEqual(len(registry.publications), 1)


if __name__ == "__main__":
    unittest.main()
