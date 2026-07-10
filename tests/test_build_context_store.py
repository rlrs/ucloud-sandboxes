from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from io import BytesIO
import os
from pathlib import Path
import stat
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.build_context_store import (
    BuildContextBlobStore,
    ContentLengthReader,
)


def _digest(payload: bytes) -> str:
    return f"sha256:{sha256(payload).hexdigest()}"


class BuildContextBlobStoreTests(unittest.TestCase):
    def test_content_length_reader_does_not_consume_the_next_request(self) -> None:
        with TemporaryDirectory() as raw_dir:
            payload = b"body"
            source = BytesIO(payload + b"next-request")
            store = BuildContextBlobStore(Path(raw_dir), max_blob_bytes=8)

            store.put(
                _digest(payload),
                ContentLengthReader(source, len(payload)),
                content_length=len(payload),
            )

            self.assertEqual(source.read(), b"next-request")

    def test_put_validates_digest_length_and_stream_boundary(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = BuildContextBlobStore(Path(raw_dir), max_blob_bytes=8)
            payload = b"context"
            digest = _digest(payload)

            path = store.put(digest, BytesIO(payload), content_length=len(payload))

            self.assertEqual(path, store.path(digest))
            self.assertEqual(store.size(digest), len(payload))
            with store.open(digest) as handle:
                self.assertEqual(handle.read(), payload)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

            invalid_cases = (
                (digest, payload, len(payload) - 1, "past Content-Length"),
                (digest, payload[:-1], len(payload), "ended early"),
                (_digest(b"different"), payload, len(payload), "digest mismatch"),
            )
            for declared, body, length, message in invalid_cases:
                with self.subTest(message=message):
                    with self.assertRaisesRegex(ValueError, message):
                        store.put(declared, BytesIO(body), content_length=length)

            with self.assertRaisesRegex(ValueError, "limit is 8"):
                store.put(digest, BytesIO(payload), content_length=9)

    def test_rejects_non_normalized_digests(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = BuildContextBlobStore(Path(raw_dir), max_blob_bytes=10)
            valid = _digest(b"x")
            invalid = (
                valid.upper(),
                valid.removeprefix("sha256:"),
                f"sha256:../{valid[-59:]}",
                f" sha256:{valid[-64:]}",
            )
            for digest in invalid:
                with self.subTest(digest=digest):
                    with self.assertRaisesRegex(ValueError, "normalized sha256"):
                        store.path(digest)

    def test_verified_existing_blob_is_deduplicated(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = BuildContextBlobStore(Path(raw_dir), max_blob_bytes=100)
            payload = b"same content"
            digest = _digest(payload)
            first = store.put(digest, BytesIO(payload), content_length=len(payload))
            first_inode = first.stat().st_ino
            first_mtime_ns = first.stat().st_mtime_ns
            first.chmod(0o644)

            result = store.put_with_status(
                digest, BytesIO(payload), content_length=len(payload)
            )
            second = result.path

            self.assertTrue(result.deduplicated)
            self.assertEqual(second.stat().st_ino, first_inode)
            self.assertEqual(second.stat().st_mtime_ns, first_mtime_ns)
            self.assertEqual(stat.S_IMODE(second.stat().st_mode), 0o600)

    def test_corrupt_existing_blob_is_atomically_replaced(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = BuildContextBlobStore(Path(raw_dir), max_blob_bytes=100)
            payload = b"expected"
            digest = _digest(payload)
            path = store.path(digest)
            path.write_bytes(b"corrupt")

            result = store.put_with_status(
                digest, BytesIO(payload), content_length=len(payload)
            )

            self.assertFalse(result.deduplicated)
            self.assertEqual(path.read_bytes(), payload)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_concurrent_writers_publish_one_complete_blob(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            payload = b"concurrent payload" * 1000
            digest = _digest(payload)
            stores = (
                BuildContextBlobStore(root, max_blob_bytes=len(payload)),
                BuildContextBlobStore(root, max_blob_bytes=len(payload)),
            )

            with ThreadPoolExecutor(max_workers=2) as executor:
                paths = list(
                    executor.map(
                        lambda store: store.put(
                            digest,
                            BytesIO(payload),
                            content_length=len(payload),
                        ),
                        stores,
                    )
                )

            self.assertEqual(paths[0], paths[1])
            self.assertEqual(paths[0].read_bytes(), payload)
            self.assertEqual(
                [path.name for path in (root / "sha256").iterdir()],
                [digest.removeprefix("sha256:")],
            )

    def test_touch_and_gc_apply_age_and_lru_limits(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = BuildContextBlobStore(
                Path(raw_dir),
                max_blob_bytes=100,
                max_total_bytes=5,
                max_entries=2,
                max_age_seconds=100,
            )
            payloads = (b"old", b"lru", b"new")
            digests = tuple(_digest(payload) for payload in payloads)
            for payload, digest in zip(payloads, digests):
                store.put(digest, BytesIO(payload), content_length=len(payload))
            os.utime(store.path(digests[0]), (800, 800))
            os.utime(store.path(digests[1]), (950, 950))
            os.utime(store.path(digests[2]), (975, 975))

            result = store.gc(protected={digests[1]}, now=1000)

            self.assertEqual(result.removed_entries, 2)
            self.assertEqual(result.removed_bytes, 6)
            self.assertEqual(result.remaining_entries, 1)
            self.assertEqual(result.remaining_bytes, 3)
            self.assertTrue(store.path(digests[1]).exists())
            self.assertFalse(store.path(digests[0]).exists())
            self.assertFalse(store.path(digests[2]).exists())

            before = store.path(digests[1]).stat().st_mtime_ns
            store.touch(digests[1])
            self.assertGreater(store.path(digests[1]).stat().st_mtime_ns, before)

    def test_gc_never_evicts_protected_blobs_to_satisfy_limits(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = BuildContextBlobStore(
                Path(raw_dir),
                max_blob_bytes=100,
                max_total_bytes=0,
                max_entries=0,
                max_age_seconds=0,
            )
            payload = b"protected"
            digest = _digest(payload)
            store.put(digest, BytesIO(payload), content_length=len(payload))

            result = store.gc(protected={digest}, now=10**10)

            self.assertEqual(result.removed_entries, 0)
            self.assertEqual(result.remaining_entries, 1)
            self.assertEqual(result.remaining_bytes, len(payload))
            self.assertTrue(store.path(digest).exists())

    def test_gc_evicts_oldest_unprotected_blob_first(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = BuildContextBlobStore(
                Path(raw_dir),
                max_blob_bytes=100,
                max_entries=2,
            )
            payloads = (b"oldest", b"middle", b"newest")
            digests = tuple(_digest(payload) for payload in payloads)
            for index, (payload, digest) in enumerate(zip(payloads, digests)):
                store.put(digest, BytesIO(payload), content_length=len(payload))
                os.utime(store.path(digest), (100 + index, 100 + index))

            result = store.gc()

            self.assertEqual(result.removed_entries, 1)
            self.assertFalse(store.path(digests[0]).exists())
            self.assertTrue(store.path(digests[1]).exists())
            self.assertTrue(store.path(digests[2]).exists())


if __name__ == "__main__":
    unittest.main()
