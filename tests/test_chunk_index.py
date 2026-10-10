"""ucloud-chunk-index: SigV4 presigning, the SQLite index and its HTTP API."""
import calendar
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest
from urllib.parse import parse_qs, urlsplit

from types import SimpleNamespace
from unittest.mock import patch

from tests.chunk_store_support import IndexFixture, ObjectServer
from ucloud_sandboxes import chunk_index
from ucloud_sandboxes.chunk_index import (BUSY, CHUNK_RESERVE_SECONDS, KNOWN, RESERVED, ChunkIndexClient,
                                          ChunkIndexService, ChunkObjectStore, MissingChunks, S3Presigner,
                                          http_range, redact, retried)
from ucloud_sandboxes.chunk_store import RAW, PackWriter, chunk_map_key, encode_tail, pack_key
from ucloud_sandboxes.managed_registry import RegistryRequestError

TEST_TIER = "contract"


class PresignerTests(unittest.TestCase):
    def test_aws_documented_vector(self):
        # docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
        presigner = S3Presigner("https://s3.amazonaws.com", "examplebucket", "us-east-1",
                                "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
        url = presigner.url("test.txt", expires=86400, now=calendar.timegm((2013, 5, 24, 0, 0, 0)))
        self.assertTrue(url.startswith("https://examplebucket.s3.amazonaws.com/test.txt?"))
        self.assertTrue(url.endswith("X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"))

    def test_matches_botocore_for_hetzner_style_keys(self):
        import botocore.session
        from botocore.config import Config
        for style, key in (("virtual", "production/chunks/packs/ab/" + "ab" * 32 + ".pack"),
                           ("path", "odd key/with+plus~and%percent")):
            client = botocore.session.get_session().create_client(
                "s3", region_name="hel1", aws_access_key_id="AKID", aws_secret_access_key="secret/key+",
                aws_session_token="token/=", endpoint_url="https://hel1.your-objectstorage.com",
                config=Config(signature_version="s3v4", s3={"addressing_style": style}))
            expected = client.generate_presigned_url("get_object", Params={"Bucket": "bucket", "Key": key},
                                                     ExpiresIn=600)
            signed_at = parse_qs(urlsplit(expected).query)["X-Amz-Date"][0]
            ours = S3Presigner("https://hel1.your-objectstorage.com", "bucket", "hel1", "AKID", "secret/key+",
                               security_token="token/=", path_style=style == "path").url(
                key, expires=600, now=calendar.timegm(time.strptime(signed_at, "%Y%m%dT%H%M%SZ")))
            with self.subTest(style=style):
                self.assertEqual(sorted(parse_qs(urlsplit(ours).query).items()),
                                 sorted(parse_qs(urlsplit(expected).query).items()))
                self.assertEqual(redact(ours), redact(expected))

    def test_lifetime_is_bounded(self):
        presigner = S3Presigner("https://s3", "b", "r", "a", "s")
        for expires in (0, 8 * 86400):
            with self.assertRaises(ValueError):
                presigner.url("k", expires=expires)


class RetryTests(unittest.TestCase):
    def test_presigned_reads_retry_transport_errors_and_5xx_only(self):
        waits, failures = [], [OSError("read timeout"), RegistryRequestError(503, "GET", "u", "slow down")]

        def read(url):
            if failures:
                raise failures.pop(0)
            return url
        self.assertEqual(retried(read, sleep=waits.append)("ok"), "ok")
        self.assertEqual(waits, [0.5, 1.0])  # M1 gate run 4: one S3 read timeout failed a commit.
        missing = retried(lambda: (_ for _ in ()).throw(RegistryRequestError(404, "GET", "u", "")), sleep=waits.append)
        with self.assertRaises(RegistryRequestError):
            missing()
        self.assertEqual(len(waits), 2)  # A 404 is an answer, not a fault.
        with self.assertRaises(OSError):
            retried(lambda: (_ for _ in ()).throw(OSError("down")), sleep=waits.append)()
        self.assertEqual(waits[2:], [0.5, 1.0, 2.0])


class ObjectStoreReadTests(unittest.TestCase):
    """Object-store reads against Hetzner's tail: stalls, spurious 405s, and
    metadata named by its digest, which never needs reading twice."""

    def setUp(self):
        self.objects = ObjectServer()
        self.addCleanup(self.objects.close)
        self.store = ChunkObjectStore(self.objects, self.objects.presigner, "test/chunks")

    def meta(self, suffix, payload):
        key = f"test/chunks/meta/{hashlib.sha256(payload + suffix.encode()).hexdigest()}{suffix}"
        self.objects.objects[key] = payload
        return key

    def test_a_stalled_read_is_abandoned_and_retried_at_once(self):
        key = self.meta(".tail", b"tail" * 100)
        stalled = []
        self.objects.fault = lambda *_: None if stalled else stalled.append(1) or ("stall", 3)
        started = time.monotonic()
        with patch.object(chunk_index, "STALL_SECONDS", 0.3):
            self.assertEqual(self.store.get(key, 1 << 20), b"tail" * 100)
        self.assertLess(time.monotonic() - started, 2.5)  # 0.3 s stall + 0.5 s backoff, not 3 s.
        self.assertEqual(len(self.objects.gets(".tail")), 2)

    def test_a_spurious_405_is_retried(self):
        key = self.meta(".tail", b"x" * 10)
        answered = []
        self.objects.fault = lambda *_: None if answered else answered.append(1) or ("status", 405)
        with patch("time.sleep"):
            self.assertEqual(self.store.get(key, 100), b"x" * 10)

    def test_bootstraps_and_chunk_maps_are_read_once_and_existence_always_asked(self):
        boot, other = self.meta(".boot.zst", b"boot" * 50), self.meta(".tail", b"tail" * 50)
        for _ in range(3):
            self.assertEqual(self.store.get(boot, 1 << 20), b"boot" * 50)
            self.store.get(other, 1 << 20)
        self.assertEqual(len(self.objects.gets(".boot.zst")), 1)
        self.assertEqual(len(self.objects.gets(".tail")), 3)  # Only digest-named metadata is cached.
        written = "test/chunks/meta/" + hashlib.sha256(b"map").hexdigest() + ".map"
        self.assertTrue(self.store.put_bytes(written, b"map"))
        self.assertEqual(self.store.get(written, 100), b"map")
        self.assertEqual(self.objects.gets(".map"), [])  # Written here, so never read back.
        del self.objects.objects[written]
        self.assertIsNone(self.store.size(written))  # Durability is never answered from the cache.


class IndexTailCacheTests(unittest.TestCase):
    """Registration reads a blob's tail table and checks its layout once:
    every task image on a base repeats the base's blobs."""

    def test_base_blobs_are_read_once_across_registrations(self):
        blobs = [hashlib.sha256(name).hexdigest() for name in (b"base-1", b"base-2", b"task")]
        tables = {blob: encode_tail([(0, 10, hashlib.sha256(blob.encode()).digest(), False)], b"")
                  for blob in blobs}
        reads, stats, puts = [], [], []

        class Store:
            prefix = "test/chunks"

            def get(self, key, limit):
                return b"boot"

            def size(self, key):
                stats.append(key.rsplit("/", 1)[-1])
                blob = key.rsplit("/", 1)[-1].split(".")[0]
                return len(tables[blob]) if key.endswith(".tail") else None

            def url(self, key):
                return key

            def reader(self, url, start, length):
                reads.append(url)
                return tables[url.rsplit("/", 1)[-1].split(".")[0]][start:start + length]

            def put_bytes(self, key, payload):
                puts.append(key)

        index = SimpleNamespace(register=lambda *args, **kwargs: {"epoch": 1})
        service = ChunkIndexService(index, Store(), store_url="http://store")
        service.chunk_map = lambda *args: SimpleNamespace(ids=[])
        service.locate = lambda ids, **kwargs: SimpleNamespace(encode=lambda: b"layout")
        service.locator = lambda component: b"locator"
        devices = {"first": blobs[:2], "second": blobs}
        for name, members in devices.items():
            bootstrap = SimpleNamespace(devices=[(blob,) for blob in members])
            with patch.object(chunk_index, "zstd_decompress", lambda data, size: b"boot"), \
                    patch.object(chunk_index, "zstd_content_size", lambda data, limit: 4), \
                    patch.object(chunk_index, "content_digest", lambda data: "sha256:" + "0" * 64), \
                    patch.object(chunk_index, "parse_bootstrap", lambda data: bootstrap):
                service.register({"component": "sha256:" + "1" * 64, "bootstrap": "sha256:" + "0" * 64,
                                  "chunk_map": {"digest": "sha256:" + "2" * 64, "size": 1}})
        self.assertEqual(len(reads), 2 * len(blobs))  # Header and table, once per blob.
        self.assertEqual(len(puts), len(blobs))  # One layout per blob, written once.
        self.assertEqual(len(stats), 2 * len(blobs))  # A tail's and a layout's existence, once each.


class IndexTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.objects = ObjectServer()
        self.addCleanup(self.objects.close)
        self.fixture = IndexFixture(self.root, self.objects)
        self.addCleanup(self.fixture.close)

    def pack(self, chunks, name="pack"):
        writer = PackWriter(self.root / name)
        for data in chunks:
            writer.add(hashlib.sha256(data).digest(), data, len(data), RAW)
        digest, size = writer.finish()
        self.objects.put_file(pack_key(self.fixture.store.prefix, digest), writer.path, sha256=digest)
        return {"digest": digest, "size": size}

    def test_range_reader_through_presigned_urls(self):
        self.objects.objects["k/x"] = bytes(range(256)) * 10
        url = self.fixture.store.url("k/x")
        self.assertEqual(http_range(url, 5, 10), bytes(range(5, 15)))
        self.assertEqual(http_range(url, None, 6), (bytes(range(256)) * 10)[-6:])
        tampered = url.replace("X-Amz-Expires=3600", "X-Amz-Expires=3601")
        with self.assertRaises(RegistryRequestError) as raised:
            http_range(tampered, 0, 1)
        self.assertEqual(raised.exception.status_code, 403)
        self.assertNotIn("X-Amz-Signature", str(raised.exception))  # URLs are credentials.
        with self.assertRaises(ValueError):
            http_range(url, 2550, 20)  # A short range is never accepted.

    def test_reserve_commit_dedupe_and_layer_claims(self):
        writer, a, b = self.fixture.writer, b"a" * 5000, b"b" * 7000
        ids = [hashlib.sha256(a).digest(), hashlib.sha256(b).digest()]
        self.assertEqual(writer.reserve(ids, "one"), [RESERVED, RESERVED])
        diff_id = "sha256:" + "d" * 64
        self.assertEqual(writer.claim_layer(diff_id, "conv", "one")["state"], "claimed")
        self.assertEqual(writer.claim_layer(diff_id, "conv", "two")["state"], "busy")
        self.assertEqual(writer.claim_layer(diff_id, "conv", "one")["state"], "claimed")  # The owner's rerun.
        first = self.pack([a, b], "first")
        with self.assertRaises(RegistryRequestError):  # The layer bootstrap is not durable yet.
            writer.commit([first], {"diff_id": diff_id, "converter": "conv", "bootstrap": "sha256:" + "e" * 64})
        self.objects.objects[f"{self.fixture.store.prefix}/meta/{'e' * 64}.boot.zst"] = b"boot"
        self.assertEqual(writer.commit([first], {"diff_id": diff_id, "converter": "conv",
                                                 "bootstrap": "sha256:" + "e" * 64})["chunks_inserted"], 2)
        self.assertEqual(writer.claim_layer(diff_id, "conv", "two"), {"state": "complete",
                                                                     "bootstrap": "sha256:" + "e" * 64})
        # A racing builder's pack: the first committed copy wins.
        second = self.pack([b, b"c" * 100], "second")
        self.assertEqual(writer.commit([second])["chunks_inserted"], 1)
        locator = writer.locate(ids)
        self.assertEqual({locator.packs[entry[0]][0] for entry in locator.entries}, {first["digest"]})
        # A pack must be durable before any row names it.
        with self.assertRaises(RegistryRequestError):
            writer.commit([{"digest": "f" * 64, "size": 100}])

    def test_chunk_reservations_hold_once_renew_lapse_and_clear_on_commit(self):
        writer, index, data = self.fixture.writer, self.fixture.index, [b"r" * 300, b"s" * 400]
        ids = [hashlib.sha256(item).digest() for item in data]
        self.assertEqual(writer.reserve(ids, "one"), [RESERVED, RESERVED])
        self.assertEqual(writer.reserve(ids[:1], "two"), [BUSY])
        self.assertEqual(writer.reserve(ids, "one"), [RESERVED, RESERVED])  # The holder renews.
        later = time.time() + CHUNK_RESERVE_SECONDS + 1
        self.assertEqual(index.reserve(ids[1:], "two", now=later), [RESERVED])  # one's hold lapsed.
        writer.commit([self.pack(data[:1], "held")])
        self.assertEqual(writer.reserve(ids, "two"), [KNOWN, RESERVED])
        rows = index._reader().execute("SELECT owner FROM reservations").fetchall()
        self.assertEqual(rows, [("two",)])  # A commit clears its chunks' holds.
        with self.assertRaises(RegistryRequestError):  # Readers cannot reserve.
            self.fixture.reader.reserve(ids, "two")

    def test_register_requires_every_chunk_and_serves_locators(self):
        from tests.test_chunk_store_formats import bootstrap
        from ucloud_sandboxes.chunk_store import chunk_map_from_bootstrap, parse_bootstrap
        from tests.chunk_store_support import pseudo_random
        from ucloud_sandboxes.chunk_store import bootstrap_key, zstd_compress
        chunk_map = chunk_map_from_bootstrap(parse_bootstrap(bootstrap()))
        encoded = chunk_map.encode()
        map_digest = "sha256:" + hashlib.sha256(encoded).hexdigest()
        self.objects.objects[chunk_map_key(self.fixture.store.prefix, map_digest[7:])] = encoded
        component, boot = "sha256:" + "c" * 64, "sha256:" + hashlib.sha256(bootstrap()).hexdigest()
        # Registration reads the bootstrap for its blobs' tails (none here).
        self.objects.objects[bootstrap_key(self.fixture.store.prefix, boot[7:])] = zstd_compress(bootstrap())
        descriptor = {"digest": map_digest, "size": len(encoded)}
        with self.assertRaises(RegistryRequestError) as raised:
            self.fixture.writer.register(component, boot, descriptor)
        self.assertEqual(raised.exception.status_code, 409)
        chunks = [pseudo_random(f"{index}:{offset}", size) for index, offset, size in
                  ((0, 0, 4096), (0, 4096, 9), (1, 0, 300))]
        self.fixture.writer.commit([self.pack(chunks)])
        self.assertEqual(self.fixture.writer.register(component, boot, descriptor), {"epoch": 1})
        self.assertEqual(self.fixture.writer.register(component, boot, descriptor), {"epoch": 1})  # Idempotent.
        with self.assertRaises(RegistryRequestError):  # Readers cannot write.
            ChunkIndexClient(self.fixture.url, "r" * 32).register(component, boot, descriptor)
        with self.assertRaises(RegistryRequestError):
            ChunkIndexClient(self.fixture.url, "x" * 32).locator(component)
        locator = self.fixture.reader.locator(component)
        self.assertEqual((locator.epoch, len(locator.entries)), (1, 3))
        self.assertEqual(set(locator.meta), {"bootstrap", "chunk_map"})
        for entry, data in zip(locator.entries, chunks):
            self.assertEqual(http_range(locator.packs[entry[0]][1], entry[1], entry[2]), data)

    def test_condemned_chunks_are_unknown_and_block_registration(self):
        data = b"z" * 100
        self.fixture.writer.commit([self.pack([data])])
        chunk_id = hashlib.sha256(data).digest()
        self.fixture.index._writer.execute("UPDATE chunks SET condemned = 1")
        self.assertEqual(self.fixture.writer.reserve([chunk_id], "one"), [RESERVED])
        with self.assertRaises(MissingChunks):
            self.fixture.service.locate([chunk_id])
        # Rewriting the chunk in a new pack revives it at the new location.
        self.fixture.writer.commit([self.pack([data], "again")])
        self.assertEqual(self.fixture.writer.reserve([chunk_id], "one"), [KNOWN])


if __name__ == "__main__":
    unittest.main()
