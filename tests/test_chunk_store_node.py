"""ucloud-chunk-store (C2.6) against an S3 stand-in with injected latency,
stalls and errors; locators naming it; workers reading only through it."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import random
import threading
import time
import unittest
from tempfile import TemporaryDirectory

from tests.chunk_store_support import REPOSITORY, ChunkStoreFixture, ObjectServer, sample_images
from ucloud_sandboxes.chunk_index import http_range, http_request
from ucloud_sandboxes.chunk_store_node import (ChunkStoreClient, ChunkStoreNode, ChunkStoreServer, ExtentCache,
                                               S3Source, locator_objects)
from ucloud_sandboxes.environment_artifact import load_environment
from ucloud_sandboxes.environment_cache import VerifiedEnvironmentCache
from ucloud_sandboxes.environment_rafs import load_rafs_image, store_access, store_locator
from ucloud_sandboxes.managed_registry import RegistryRequestError

TEST_TIER = "contract"
MIB = 1024 ** 2
PREFIX = "test/chunks"
READ, WRITE = "r" * 32, "w" * 32


def pack_object(seed, size):
    data = random.Random(seed).randbytes(size)
    digest = hashlib.sha256(data).hexdigest()
    return f"packs/{digest[:2]}/{digest}.pack", data


class StoreNode:
    """A store node on localhost over ObjectServer; restartable on one cache."""

    def __init__(self, test, objects, root, *, budget=64 * MIB, extent=MIB, concurrency=8, deadline=10.0):
        self.test, self.objects, self.root = test, objects, Path(root)
        self.budget, self.extent, self.concurrency, self.deadline = budget, extent, concurrency, deadline
        self.start()

    def start(self):
        source = S3Source(self.objects.presigner, PREFIX, concurrency=self.concurrency, deadline=self.deadline)
        self.node = ChunkStoreNode(ExtentCache(self.root / "cache", self.budget), source, extent_bytes=self.extent,
                                   warm_concurrency=2)
        self.server = ChunkStoreServer(("127.0.0.1", 0), self.node, read_token=READ, write_token=WRITE)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.test.addCleanup(self.stop)

    def stop(self):
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
            self.node.close()
            self.server = None

    def get(self, key, start, length, token=READ):
        return http_range(f"{self.url}/v1/objects/{key}", start, length, headers={"Authorization": "Bearer " + token})

    def metrics(self):
        return ChunkStoreClient(self.url, READ).metrics()


class ChunkStoreNodeTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.objects = ObjectServer()
        self.addCleanup(self.objects.close)
        self.packs = dict(pack_object(seed, size) for seed, size in ((1, 3 * MIB + 5000), (2, 2 * MIB), (3, 700_000)))
        for key, data in self.packs.items():
            self.objects.objects[f"{PREFIX}/{key}"] = data
        self.keys = list(self.packs)

    def s3_gets(self):
        return len(self.objects.gets())

    def test_ranges_read_through_once_then_hit_from_disk(self):
        store, key = StoreNode(self, self.objects, self.root), self.keys[0]
        data = self.packs[key]
        self.assertEqual(store.get(key, MIB - 100, 300), data[MIB - 100:MIB + 200])  # Spans two extents.
        self.assertEqual(self.s3_gets(), 2)
        self.assertEqual(store.get(key, MIB - 10, 20), data[MIB - 10:MIB + 10])
        self.assertEqual(http_range(f"{store.url}/v1/objects/{key}", None, 1000, headers={
            "Authorization": "Bearer " + READ}), data[-1000:])  # A suffix: the last extent.
        _, _, whole = http_request("GET", f"{store.url}/v1/objects/{self.keys[2]}", max_bytes=MIB,
                                   headers={"Authorization": "Bearer " + READ})
        self.assertEqual(whole, self.packs[self.keys[2]])
        self.assertEqual(self.s3_gets(), 4)
        metrics = store.metrics()
        self.assertEqual((metrics["hits"], metrics["misses"], metrics["s3"]["requests"]), (1, 3, 4))
        self.assertEqual(metrics["cache"]["extents"], 4)
        self.assertIsNotNone(metrics["s3"]["ttfb"]["p99_ms"])
        for path, token, status in ((f"/v1/objects/{key}", "x" * 32, 401), (f"/v1/objects/{key}", "", 401),
                                    ("/v1/objects/packs/00/" + "0" * 64 + ".pack", READ, 404),
                                    ("/v1/objects/meta/secret.env", READ, 404), ("/v1/warm/1", READ, 401)):
            with self.subTest(path=path), self.assertRaises(RegistryRequestError) as caught:
                http_request("GET", store.url + path, max_bytes=MIB, headers={"Authorization": "Bearer " + token})
            self.assertEqual(caught.exception.status_code, status)
        with self.assertRaises(RegistryRequestError) as caught:
            store.get(key, len(data) + 10, 10)
        self.assertEqual(caught.exception.status_code, 416)
        with self.assertRaises(RegistryRequestError) as caught:  # Workers cannot warm.
            ChunkStoreClient(store.url, READ).warm([{"key": key}])
        self.assertEqual(caught.exception.status_code, 401)

    def test_concurrent_misses_on_one_extent_coalesce(self):
        store, key = StoreNode(self, self.objects, self.root), self.keys[0]
        self.objects.fault = lambda *_: ("stall", .3)
        with ThreadPoolExecutor(16) as pool:
            results = list(pool.map(lambda offset: store.get(key, offset, 4096), range(0, 16 * 4096, 4096)))
        self.assertEqual(results, [self.packs[key][offset:offset + 4096] for offset in range(0, 16 * 4096, 4096)])
        self.assertEqual(self.s3_gets(), 1)
        self.assertGreaterEqual(store.metrics()["coalesced"], 14)

    def test_a_stalled_request_is_hedged_and_the_loser_leaves_nothing(self):
        store, key = StoreNode(self, self.objects, self.root), self.keys[1]
        calls = []
        self.objects.fault = lambda *_: calls.append(1) or (("stall", 2.5) if len(calls) == 1 else None)
        began = time.monotonic()
        self.assertEqual(store.get(key, 0, 4096), self.packs[key][:4096])
        self.assertLess(time.monotonic() - began, 1.5)  # Hedged after about 0.18 s, not 2.5 s.
        s3 = store.metrics()["s3"]
        self.assertEqual((s3["hedged"], s3["hedge_wins"], s3["requests"]), (1, 1, 2))
        # A stall mid-body is hedged too.
        self.objects.fault = lambda *_: calls.append(1) or (("body_stall", 2.5) if len(calls) == 3 else None)
        self.assertEqual(store.get(key, MIB, 4096), self.packs[key][MIB:MIB + 4096])
        self.assertEqual(store.metrics()["s3"]["hedge_wins"], 2)
        deadline = time.monotonic() + 8
        while any((self.root / "cache" / "tmp").iterdir()) and time.monotonic() < deadline:
            time.sleep(.1)  # The losers drop their partial files.
        self.assertEqual(list((self.root / "cache" / "tmp").iterdir()), [])

    def test_builders_hold_half_the_s3_slots_so_worker_fills_never_wait_behind_them(self):
        store, slow, fast = StoreNode(self, self.objects, self.root, concurrency=4), self.keys[0], self.keys[1]
        self.objects.fault = lambda key, *_: ("stall", 1.5) if key.endswith(slow) else None
        with ThreadPoolExecutor(4) as pool:  # A converter verifying: four stalled extents, write token.
            builders = [pool.submit(store.get, slow, index * MIB, 4096, WRITE) for index in range(4)]
            time.sleep(.3)
            began = time.monotonic()
            self.assertEqual(store.get(fast, 0, 4096), self.packs[fast][:4096])  # A worker, read token.
            self.assertLess(time.monotonic() - began, 1.0)
            self.assertEqual(len(self.objects.gets(slow)), 2)  # Two background slots, never hedged.
            self.assertEqual([future.result() for future in builders],
                             [self.packs[slow][index * MIB:index * MIB + 4096] for index in range(4)])

    def test_s3_errors_are_retried_then_answered_503(self):
        store, key = StoreNode(self, self.objects, self.root, deadline=1.5), self.keys[0]
        answers = iter([("status", 503), ("status", 500)])
        self.objects.fault = lambda *_: next(answers, None)
        self.assertEqual(store.get(key, 0, 100), self.packs[key][:100])
        self.assertEqual(store.metrics()["s3"]["retries"], 2)
        self.objects.fault = lambda *_: ("status", 503)
        with self.assertRaises(RegistryRequestError) as caught:
            store.get(key, 2 * MIB, 100)
        self.assertEqual(caught.exception.status_code, 503)  # Retryable for workers, then EIO.
        self.objects.fault = None
        self.assertEqual(store.get(key, 2 * MIB, 100), self.packs[key][2 * MIB:2 * MIB + 100])

    def test_a_pack_that_does_not_match_its_name_is_never_cached(self):
        store, key = StoreNode(self, self.objects, self.root, extent=4 * MIB), self.keys[1]
        self.objects.corrupt[f"{PREFIX}/{key}"] = bytes(len(self.packs[key]))
        with self.assertRaises(RegistryRequestError) as caught:
            store.get(key, 0, 100)
        self.assertEqual(caught.exception.status_code, 502)
        metrics = store.metrics()
        self.assertEqual((metrics["fill_rejects"], metrics["cache"]["extents"]), (1, 0))
        del self.objects.corrupt[f"{PREFIX}/{key}"]
        self.assertEqual(store.get(key, 0, 100), self.packs[key][:100])

    def test_the_cache_keeps_its_byte_budget_least_recently_used_first(self):
        store, (a, b, _) = StoreNode(self, self.objects, self.root, budget=3 * MIB), self.keys
        for offset in (0, MIB, 2 * MIB):
            store.get(a, offset, 10)
        store.get(a, 0, 10)  # a/0 is now the most recent.
        store.get(b, 0, 10)
        cache = store.node.cache.stats()
        self.assertLessEqual(cache["bytes"], 3 * MIB)
        self.assertEqual(cache["evictions"], 1)
        before = self.s3_gets()
        store.get(a, 0, 10)
        store.get(a, 2 * MIB, 10)
        self.assertEqual(self.s3_gets(), before)
        store.get(a, MIB, 10)  # The evicted extent: least recently used.
        self.assertEqual(self.s3_gets(), before + 1)

    def test_a_builders_reads_never_evict_the_warm_set(self):
        # Before M2 wave 2: converters verify through the node, and a wave is
        # larger than the cache; their extents go first, the warm set stays.
        store, (a, b, c) = StoreNode(self, self.objects, self.root, budget=3 * MIB), self.keys
        store.get(a, 0, 10)
        store.get(a, MIB, 10)  # Two warm extents (a worker).
        for offset in (0, MIB, 0, MIB):  # A builder reads b twice over: no promotion.
            store.get(b, offset, 10, WRITE)
        store.get(c, 0, 10, WRITE)
        before = self.s3_gets()
        store.get(a, 0, 10)
        store.get(a, MIB, 10)
        self.assertEqual(self.s3_gets(), before)  # Still cached.
        cache = store.node.cache
        self.assertEqual([ident[0] for ident in cache._lru][-2:], [a.split("/")[-1][:-5]] * 2)
        b_digest = b.split("/")[-1][:-5]
        warm = ChunkStoreClient(store.url, WRITE)
        self.assertEqual(warm.wait(warm.warm([{"key": c, "ranges": None}])["job"], timeout=30)["failed"], 0)
        self.assertNotIn(c.split("/")[-1][:-5], [ident[0] for ident in list(cache._lru)[:1]])  # Warm promotes.
        self.assertEqual(store.get(b, 0, 10, WRITE), self.packs[b][:10])  # A full warm cache still serves it.
        self.assertEqual(next(iter(cache._lru))[0], b_digest)
        store.stop()
        store.start()  # A restart orders by mtime: the builder's cold extent stays first.
        self.assertEqual(next(iter(store.node.cache._lru))[0], b_digest)

    def test_a_restart_drops_torn_and_partial_files_and_never_serves_them(self):
        store, key = StoreNode(self, self.objects, self.root), self.keys[0]
        store.get(key, 0, 10)
        store.get(key, MIB, 10)
        store.stop()
        cache = self.root / "cache"
        (cache / "tmp" / "partial-fill").write_bytes(b"x" * 1000)
        (cache / "ab").mkdir(exist_ok=True)
        (cache / "ab" / "junk").write_bytes(b"x")
        first = next(path for path in cache.rglob("*.pack.0.*"))
        torn = bytearray(first.read_bytes())
        torn[123] ^= 0xFF  # Same size, wrong bytes: what a lost page looks like.
        first.write_bytes(torn)
        store.start()
        self.assertEqual(list((cache / "tmp").iterdir()), [])
        self.assertFalse((cache / "ab" / "junk").exists())
        before = self.s3_gets()
        self.assertEqual(store.get(key, 100, 50), self.packs[key][100:150])
        self.assertEqual(store.get(key, MIB, 50), self.packs[key][MIB:MIB + 50])  # Intact: hashed, then served.
        self.assertEqual(self.s3_gets(), before + 1)
        self.assertEqual(store.metrics()["cache"]["verify_failures"], 1)

    def test_warm_fills_objects_and_ranges_with_bounded_concurrency(self):
        store = StoreNode(self, self.objects, self.root)
        self.objects.fault = lambda *_: ("stall", .05)
        client = ChunkStoreClient(store.url, WRITE)
        job = client.warm([{"key": self.keys[0]}, {"key": self.keys[1], "ranges": [[MIB + 10, 100]]},
                           {"key": self.keys[2], "ranges": None}], concurrency=2)
        progress = client.wait(job["job"], timeout=20, poll=.05)
        self.assertEqual((progress["state"], progress["done"], progress["failed"]), ("complete", 6, 0))
        self.assertEqual(progress["bytes"], len(self.packs[self.keys[0]]) + MIB + len(self.packs[self.keys[2]]))
        self.assertLessEqual(self.objects.peak, 2)
        before = self.s3_gets()
        store.get(self.keys[0], 3 * MIB, 5000)
        store.get(self.keys[1], MIB, 10)
        self.assertEqual(self.s3_gets(), before)
        again = client.wait(client.warm([{"key": self.keys[0]}])["job"], timeout=10, poll=.05)
        self.assertEqual((again["cached"], again["done"]), (4, 0))
        with self.assertRaises(RegistryRequestError):
            client.warm([{"key": "meta/../../etc/passwd"}])

    def test_index_locators_name_the_node_and_workers_read_only_through_it(self):
        fixture = ChunkStoreFixture(self)
        sample_images(fixture.client)
        root = fixture.converter.convert(REPOSITORY, "a")["root"]
        digest = load_environment(fixture.registry, root).environment.base
        component = fixture.registry.load(digest)
        presigned = load_rafs_image(digest, component, fixture.index.reader)
        self.assertTrue(all("X-Amz-Signature=" in url for _, url in presigned._locator.packs))
        store = StoreNode(self, fixture.objects, fixture.root / "node")
        fixture.index.service.store_url = store.url
        fixture.index.service._locators.clear()
        locator = fixture.index.reader.locator(digest)
        urls = [url for _, url in locator.packs] + list(locator.meta.values())
        self.assertTrue(all(url.startswith(store.url + "/v1/objects/") and "?" not in url for url in urls))
        builder = fixture.index.writer.locate(list(presigned.map.ids[:3]))  # Builders stay presigned.
        self.assertTrue(all("X-Amz-Signature=" in url for _, url in builder.packs))
        # The converter's verification mount reads through the node, as workers do.
        mapped = store_locator(fixture.index.writer.locate(list(presigned.map.ids)), store.url,
                               fixture.index.store.prefix)
        self.assertEqual(mapped.packs, locator.packs)
        reader, getter = store_access(store.url, READ)
        image = load_rafs_image(digest, component, fixture.index.reader, reader=reader, getter=getter,
                                origin=store.url)
        cache = VerifiedEnvironmentCache(fixture.root / "worker", None, max_bytes=64 * MIB)
        self.addCleanup(cache.close)
        device = cache.read(image, 0, image.image_size)
        for offset, size, chunk_id in zip(image.map.offsets, image.map.sizes, image.map.ids):
            self.assertEqual(hashlib.sha256(device[offset:offset + size]).digest(), chunk_id)
        self.assertGreater(store.metrics()["bytes_served"], 0)
        warm = locator_objects(locator, store.url)
        self.assertEqual(len(warm), len(locator.packs) + 2)
        with self.assertRaises(ValueError):  # A presigned locator is refused when a node is configured.
            load_rafs_image(digest, component, _Presigned(presigned._locator), reader=reader, origin=store.url)
        with self.assertRaises(ValueError):  # So is one a refresh brings.
            image._use(presigned._locator)
        with self.assertRaises(ValueError):
            reader(presigned._locator.packs[0][1], 0, 10)

    def test_a_worker_fails_closed_when_the_node_is_down(self):
        fixture = ChunkStoreFixture(self)
        sample_images(fixture.client)
        digest = load_environment(fixture.registry, fixture.converter.convert(REPOSITORY, "a")["root"]).environment.base
        store = StoreNode(self, fixture.objects, fixture.root / "node")
        fixture.index.service.store_url = store.url
        reader, getter = store_access(store.url, READ)
        image = load_rafs_image(digest, fixture.registry.load(digest), fixture.index.reader, reader=reader,
                                getter=getter, origin=store.url)
        store.stop()
        cache = VerifiedEnvironmentCache(fixture.root / "worker", None, max_bytes=64 * MIB, fetch_timeout_seconds=1.0)
        self.addCleanup(cache.close)
        began, before = time.monotonic(), len(fixture.objects.requests)
        with self.assertRaises((OSError, ValueError)):  # EIO at the NBD layer; never zeros.
            cache.read(image, image.map.offsets[0], 4096)
        self.assertGreater(cache.metrics()["fetch_retries"], 0)
        self.assertLess(time.monotonic() - began, 5)
        self.assertEqual(len(fixture.objects.requests), before)  # Never S3 directly.
        self.assertFalse(cache.contains(image.chunks[0]))


class _Presigned:
    """An index that answers a presigned locator, as a misconfigured one might."""

    def __init__(self, locator):
        self._locator = locator

    def locator(self, digest):
        return self._locator


class StoreNodeConfigTests(unittest.TestCase):
    NODE = {"url": "http://10.42.0.10:5091/", "listen": "0.0.0.0:5091", "cache_dir": "/var/lib/ucloud-chunk-store",
            "cache_bytes": 800 * 1024 ** 3, "extent_bytes": 8 * MIB, "s3_concurrency": 64, "serve_index": True}

    def raw(self, **changes):
        from tests.test_chunk_store_formats import ChunkStoreConfigTests
        return {**ChunkStoreConfigTests.RAW, "index_url": "http://10.42.0.10:5090", "index_listen": "0.0.0.0:5090",
                "store_node": {**self.NODE, **changes}}

    def test_strict_off_by_default_and_round_trips(self):
        from ucloud_sandboxes.environment_config import ChunkStoreConfig
        from tests.test_chunk_store_formats import ChunkStoreConfigTests
        self.assertNotIn("store_node", ChunkStoreConfig.from_dict(ChunkStoreConfigTests.RAW).to_dict())
        store = ChunkStoreConfig.from_dict(self.raw())
        self.assertEqual((store.store_node.url, store.store_node.extent_bytes), ("http://10.42.0.10:5091", 8 * MIB))
        self.assertEqual(ChunkStoreConfig.from_dict(store.to_dict()), store)
        for changes in ({"extent_bytes": 3 * MIB}, {"extent_bytes": 128 * MIB}, {"cache_bytes": 1}, {"url": "ftp://x"},
                        {"listen": "nowhere"}, {"cache_dir": "relative"}, {"serve_index": 1}, {"extra": 1},
                        {"s3_concurrency": 0}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                ChunkStoreConfig.from_dict(self.raw(**changes))
        with self.assertRaises(ValueError):  # The index must live where serve_index says.
            ChunkStoreConfig.from_dict({**self.raw(), "index_url": "http://10.42.0.2:5090"})
        self.assertIsNotNone(ChunkStoreConfig.from_dict({**self.raw(serve_index=False),
                                                         "index_url": "http://10.42.0.2:5090"}).store_node)
        # C2.1: nydusd is pinned, and reads only the store node's virtual blobs.
        nydusd = {"path": "/usr/local/libexec/nydusd", "sha256": "9" * 64}
        self.assertEqual(ChunkStoreConfig.from_dict(store.to_dict() | {"nydusd": nydusd}).to_dict()["nydusd"], nydusd)
        for bad in ({**nydusd, "sha256": "short"}, {**nydusd, "path": "nydusd"}, {"path": nydusd["path"]}):
            with self.subTest(nydusd=bad), self.assertRaises(ValueError):
                ChunkStoreConfig.from_dict({**self.raw(), "nydusd": bad})
        with self.assertRaisesRegex(ValueError, "needs store_node"):
            ChunkStoreConfig.from_dict({**ChunkStoreConfigTests.RAW, "nydusd": nydusd})

    def test_the_store_role_renders_a_node_without_fleet_services(self):
        from tests import test_vm_init as vm_fixtures
        from ucloud_sandboxes.environment_config import ChunkStoreConfig
        from ucloud_sandboxes.vm_init import render_vm_init_script
        store = ChunkStoreConfig.from_dict(self.raw())
        options = vm_fixtures.VmInitTests._options(
            role="store", chunk_store_config_json=json.dumps(store.to_dict()), chunk_store_read_token=READ,
            chunk_store_write_token=WRITE, chunk_store_s3_access_key_id="AKIDSTORENODE",
            chunk_store_s3_secret_access_key="secret-store-node-key")
        script = render_vm_init_script(options)
        for expected in ("ucloud-chunk-store.service", "ucloud-chunk-index.service", "/etc/ucloud-sandboxes/chunk-store.env",
                         "runtime/agent/node-agent-runtime.tar", "http://127.0.0.1:5091/healthz"):
            self.assertIn(expected, script)
        for absent in ("secret-store-node-key", READ, "docker", "serve-direct-node-agent", "heartbeat"):
            self.assertNotIn(absent, script)
        import subprocess
        syntax = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)
        from ucloud_sandboxes.chunk_store_node import _unit
        self.assertIn("serve-chunk-store --chunk-store-config /etc/ucloud-sandboxes/chunk-store.json",
                      _unit("d", "serve-chunk-store", "/work/bin/ucloud-sandboxes", "ucloud"))
        for invalid in ({"chunk_store_write_token": READ}, {"chunk_store_s3_secret_access_key": "has space"},
                        {"chunk_store_config_json": json.dumps(ChunkStoreConfig.from_dict(self.raw()).to_dict()
                                                               | {"store_node": None})}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                render_vm_init_script(vm_fixtures.VmInitTests._options(**{**options.__dict__, **invalid}))
        with self.assertRaisesRegex(ValueError, "store role"):
            render_vm_init_script(vm_fixtures.VmInitTests._options(chunk_store_read_token=READ))

    def test_the_index_runs_only_where_serve_index_puts_it(self):
        from types import SimpleNamespace
        from ucloud_sandboxes.chunk_convert import serve_chunk_index
        from unittest import mock
        from ucloud_sandboxes.environment_config import ChunkStoreConfig
        with TemporaryDirectory() as directory:
            path = Path(directory) / "chunk-store.json"
            path.write_text(json.dumps(self.raw(serve_index=False) | {"index_url": "http://10.42.0.2:5090"}))
            self.assertEqual(serve_chunk_index(SimpleNamespace(chunk_store_config=path, config=None)), 78)
            # On the gateway, with the index on the store node: make the tokens, then stop.
            tokens = {"read_token_file": f"{directory}/index/read.token", "write_token_file": f"{directory}/index/w"}
            store = ChunkStoreConfig.from_dict(self.raw() | tokens)
            with mock.patch("ucloud_sandboxes.chunk_convert._chunk_store", return_value=store):
                self.assertEqual(serve_chunk_index(SimpleNamespace(chunk_store_config=None, config=path)), 78)
            self.assertEqual(oct(Path(tokens["read_token_file"]).stat().st_mode & 0o777), "0o600")
            self.assertTrue(Path(tokens["write_token_file"]).exists())


if __name__ == "__main__":
    unittest.main()
