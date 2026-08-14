from __future__ import annotations

import hashlib
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from ucloud_sandboxes.storage_native_s3 import Boto3S3ObjectClient


S3_ENDPOINT = os.environ.get("UCLOUD_TEST_S3_ENDPOINT", "").rstrip("/")
S3_ACCESS_KEY = os.environ.get("UCLOUD_TEST_S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("UCLOUD_TEST_S3_SECRET_KEY", "minioadmin")
S3_BUCKET = "ucloud-contract"


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


@unittest.skipUnless(
    S3_ENDPOINT,
    "set UCLOUD_TEST_S3_ENDPOINT to run the S3-compatible contract",
)
class S3CompatibleContractTests(unittest.TestCase):
    def test_object_file_copy_listing_and_multipart_contract(self) -> None:
        import boto3
        from botocore.config import Config

        credentials = {
            "access_key_id": S3_ACCESS_KEY,
            "secret_access_key": S3_SECRET_KEY,
        }
        administration = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            region_name="us-east-1",
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            config=Config(s3={"addressing_style": "path"}),
        )
        administration.create_bucket(Bucket=S3_BUCKET)
        client = Boto3S3ObjectClient(
            endpoint=S3_ENDPOINT,
            bucket=S3_BUCKET,
            region="us-east-1",
            credentials=credentials,
            force_path_style=True,
        )

        metadata = b'{"contract":true}'
        client.put_bytes("contract/metadata.json", metadata, sha256=_digest(metadata))
        stat = client.stat("contract/metadata.json")
        self.assertIsNotNone(stat)
        assert stat is not None
        self.assertEqual(stat.size, len(metadata))
        self.assertEqual(stat.sha256, _digest(metadata))
        self.assertEqual(
            client.get_bytes("contract/metadata.json", max_bytes=len(metadata)),
            metadata,
        )
        with self.assertRaisesRegex(ValueError, "size limit"):
            client.get_bytes("contract/metadata.json", max_bytes=len(metadata) - 1)

        with TemporaryDirectory() as raw_dir:
            payload = b"file upload through boto transfer manager\n"
            path = Path(raw_dir) / "payload.bin"
            path.write_bytes(payload)
            client.put_file("contract/payload.bin", path, sha256=_digest(payload))
        self.assertEqual(
            client.get_bytes("contract/payload.bin", max_bytes=len(payload)),
            payload,
        )
        client.copy(
            "contract/payload.bin",
            "contract/copied.bin",
            size=len(payload),
        )
        self.assertEqual(
            client.get_bytes("contract/copied.bin", max_bytes=len(payload)),
            payload,
        )

        first = b"a" * (5 * 1024 * 1024)
        second = b"tail"
        upload_id = client.create_multipart_upload("contract/multipart.bin")
        parts = (
            (1, client.upload_part("contract/multipart.bin", upload_id, 1, first)),
            (2, client.upload_part("contract/multipart.bin", upload_id, 2, second)),
        )
        client.complete_multipart_upload("contract/multipart.bin", upload_id, parts)
        self.assertEqual(
            client.get_bytes(
                "contract/multipart.bin",
                max_bytes=len(first) + len(second),
            ),
            first + second,
        )

        abandoned_id = client.create_multipart_upload("contract/abandoned.bin")
        client.upload_part("contract/abandoned.bin", abandoned_id, 1, second)
        client.abort_multipart_upload("contract/abandoned.bin", abandoned_id)
        self.assertIsNone(client.stat("contract/abandoned.bin"))

        listed = dict(client.list_objects("contract/"))
        self.assertEqual(
            set(listed),
            {
                "contract/copied.bin",
                "contract/metadata.json",
                "contract/multipart.bin",
                "contract/payload.bin",
            },
        )

        for key in listed:
            client.delete(key)
        self.assertEqual(client.list_objects("contract/"), ())
        administration.delete_bucket(Bucket=S3_BUCKET)


if __name__ == "__main__":
    unittest.main()
