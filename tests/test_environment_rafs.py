"""The worker's RAFS read path against an S3 stand-in, and concurrent attach."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import threading
import time
import unittest
from unittest import mock

from tempfile import TemporaryDirectory

from tests.chunk_store_support import REPOSITORY, ChunkStoreFixture, sample_images, signing
from tests.test_environment_artifact import MemoryRegistry
from ucloud_sandboxes.chunk_store import Locator, pack_key
from ucloud_sandboxes.environment_artifact import (CHUNK_BYTES, EnvironmentArtifactRegistry, load_environment,
                                                   sign_component)
from ucloud_sandboxes.environment_backend import EnvironmentBackend
from ucloud_sandboxes.environment_cache import VerifiedEnvironmentCache
from ucloud_sandboxes.environment_rafs import DEMAND_WINDOW_BYTES, load_rafs_image
from ucloud_sandboxes.environment_trace import LocalTraceStore

TEST_TIER = "contract"


class RafsReadTests(unittest.TestCase):
    def setUp(self):
        self.store = ChunkStoreFixture(self)
        sample_images(self.store.client)
        self.roots = [self.store.converter.convert(REPOSITORY, tag)["root"] for tag in ("a", "b")]
        self.cache = self.new_cache("cache")

    def new_cache(self, name):
        cache = VerifiedEnvironmentCache(self.store.root / name, None, max_bytes=256 * 1024 ** 2)
        self.addCleanup(cache.close)
        return cache

    def image(self, root):
        digest = load_environment(self.store.registry, root).environment.base
        return load_rafs_image(digest, self.store.registry.load(digest), self.store.index.reader)

    def device(self, image, cache=None):
        cache = cache or self.cache
        return b"".join(cache.read(image, offset, min(4 * 1024 ** 2, image.image_size - offset))
                        for offset in range(0, image.image_size, 4 * 1024 ** 2))

    def test_device_serves_the_bootstrap_verified_chunks_and_zero_holes(self):
        image = self.image(self.roots[0])
        device = bytearray(self.device(image))
        size = image.bootstrap.size
        self.assertEqual(bytes(device[:size]), image.bootstrap.read(0, size))
        device[:size] = bytes(size)
        for offset, size, chunk_id in zip(image.map.offsets, image.map.sizes, image.map.ids):
            self.assertEqual(hashlib.sha256(device[offset:offset + size]).digest(), chunk_id)
            device[offset:offset + size] = bytes(size)
        self.assertEqual(device.count(0), len(device))  # Holes read as zeros.
        with self.assertRaises(ValueError):
            self.cache.read(image, image.image_size - 1, 2)

    def test_a_file_backed_bootstrap_is_checked_block_by_block(self):
        digest = load_environment(self.store.registry, self.roots[0]).environment.base
        image = load_rafs_image(digest, self.store.registry.load(digest), self.store.index.reader,
                                meta_root=self.store.root)
        path, expected = image.bootstrap.path, image.bootstrap.read(0, 8192)
        self.assertEqual(self.cache.read(image, 0, 8192), expected)
        changed = bytearray(path.read_bytes())
        changed[5000] ^= 1
        path.write_bytes(changed)
        with self.assertRaises(ValueError):
            self.cache.read(image, 4096, 4096)
        image.close()
        self.assertFalse(path.exists())

    def test_a_demand_miss_fetches_one_window_that_answers_its_neighbours(self):
        image = self.image(self.roots[0])
        middle = len(image.chunks) // 2
        before = len(self.store.objects.gets())  # The index read pack footers at commit.
        self.cache.read(image, image.map.offsets[middle], 4096)
        gets = self.store.objects.gets()[before:]
        self.assertEqual(len(gets), 1)
        first, last = map(int, gets[0][1].removeprefix("bytes=").split("-"))
        self.assertLessEqual(last - first + 1, DEMAND_WINDOW_BYTES)
        _, members = image.window(middle, lambda chunk: False)[1:]
        self.assertGreater(len(members), 1)
        for index in members:
            self.assertTrue(self.cache.contains(image.chunks[index]))
            self.cache.read(image, image.map.offsets[index], image.map.sizes[index])
        self.assertEqual(len(self.store.objects.gets()), before + 1)

    def test_the_node_cache_is_shared_across_images_by_chunk_id(self):
        first, second = self.image(self.roots[0]), self.image(self.roots[1])
        self.device(first)
        before = len(self.store.objects.gets())
        shared = [index for index, chunk in enumerate(second.chunks) if chunk.digest in set(first.chunks and
                  [chunk.digest for chunk in first.chunks])]
        self.assertGreater(len(shared), 5)
        for index in shared:
            self.cache.read(second, second.map.offsets[index], second.map.sizes[index])
        self.assertEqual(len(self.store.objects.gets()), before)
        self.device(second)
        self.assertEqual(len(self.store.objects.gets()), before + 1)  # B's own chunk.

    def test_bytes_that_do_not_verify_refetch_the_locator_once_then_fail(self):
        image = self.image(self.roots[0])
        refreshes = []
        original = image._refresh
        image._refresh = lambda: refreshes.append(1) or original()
        for key, payload in self.store.objects.objects.items():
            if key.endswith(".pack"):
                self.store.objects.corrupt[key] = payload[:16] + bytes(len(payload) - 16)
        with self.assertRaises(ValueError):
            self.cache.read(image, image.map.offsets[0], 4096)
        self.assertEqual(len(refreshes), 1)
        self.assertGreaterEqual(self.cache.metrics()["corruptions"], 1)
        self.assertFalse(self.cache.contains(image.chunks[0]))

    def test_an_expired_url_refreshes_the_locator(self):
        image = self.image(self.roots[0])
        locator, presigner = image._locator, self.store.objects.presigner
        stale = tuple((digest, presigner.url(pack_key(self.store.index.store.prefix, digest), expires=60,
                                             now=time.time() - 3600)) for digest, _ in locator.packs)
        image._use(Locator(locator.epoch, stale, locator.entries, locator.meta))
        self.assertEqual(len(self.cache.read(image, image.map.offsets[0], 4096)), 4096)
        self.assertIsNot(image._locator.packs, stale)

    def test_traces_record_chunk_ids_and_replay_as_coalesced_pack_ranges(self):
        image = self.image(self.roots[0])
        traces = LocalTraceStore(self.store.root / "traces")
        traces.save(image, [3, 1, 2])
        raw = json.loads((self.store.root / "traces" / (image.image_digest[7:] + ".json")).read_text())
        self.assertEqual(raw["chunks"], [image.chunk_ids[index] for index in (3, 1, 2)])
        self.assertEqual(traces.load(image), ("present", (3, 1, 2)))
        other = self.image(self.roots[1])
        (self.store.root / "traces" / (other.image_digest[7:] + ".json")).write_text(
            json.dumps(raw | {"image_digest": other.image_digest, "chunks": ["f" * 64]}))
        self.assertEqual(traces.load(other), ("invalid", None))
        # Replay of the whole image: pack ranges of up to 4 MiB, not chunk GETs.
        cache, before = self.new_cache("replay"), len(self.store.objects.gets())
        job = cache.prefetch(image, image.prefetch_order(range(len(image.chunks))), kind="trace",
                             max_bytes=cache.max_bytes)
        self.assertTrue(job.wait(30))
        self.assertEqual(job.fetched_chunks, len({chunk.digest for chunk in image.chunks}))
        self.assertEqual(len(self.store.objects.gets()) - before, len({entry[0] for entry in image._locator.entries}))

    def test_the_backend_attaches_rafs_components_through_the_chunk_index(self):
        digest = load_environment(self.store.registry, self.roots[0]).environment.base
        bound = []

        class Device:
            def __init__(self, path, component, cache, workers, *, trusted_keys):
                component.authenticate(trusted_keys)
                self.path = path
                bound.append(component)

            def close(self):
                pass
        backend = EnvironmentBackend(self.store.root / "backend", self.store.registry, devices=[Path("/dev/nbd-x")],
                                     device_factory=Device, mount=lambda device, target: None,
                                     unmount=lambda target: None, mounted=lambda path: False,
                                     rafs=lambda d, c: load_rafs_image(d, c, self.store.index.reader))
        self.addCleanup(backend.close)
        backend.ensure(digest)
        self.assertEqual(bound[0].image_size, bound[0].component.device_size)
        self.assertEqual(len(backend.cache.read(bound[0], 0, 8192)), 8192)
        bare = EnvironmentBackend(self.store.root / "bare", self.store.registry, devices=[Path("/dev/nbd-y")],
                                  device_factory=Device, mount=lambda device, target: None)
        self.addCleanup(bare.close)
        with self.assertRaisesRegex(RuntimeError, "chunk index"):
            bare.ensure(digest)


class ConcurrentAttachTests(unittest.TestCase):
    """Design §4 item 6: one single flight per component, nothing global."""

    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.key, trusted = signing()
        self.registry = EnvironmentArtifactRegistry(MemoryRegistry(), "environments", trusted)
        image = self.root / "image.erofs"
        image.write_bytes(b"a" * CHUNK_BYTES)
        self.digest = self.registry.publish(image, sign_component(image, source_image="sha256:" + "1" * 64,
                                                                  signing_key=self.key), tag="fixture")

    def backend(self, devices=2, attach_concurrency=2, rafs=None):
        self.loads, self.gates, self.factory_calls, self.device_gate = [], {}, [], None
        load = self.registry.load

        def gated(digest):
            self.loads.append(digest)
            gate = self.gates.get(digest)
            if gate is not None:
                gate.wait(5)
            return load(digest)
        self.registry.load = gated
        test = self

        class Device:
            def __init__(self, path, *args, **kwargs):
                test.factory_calls.append(path)
                self.path = path
                if test.device_gate is not None and len(test.factory_calls) == 1:
                    test.device_gate.wait(5)  # The first bind is slow (a cold miss path).

            def close(self):
                pass
        backend = EnvironmentBackend(self.root / "backend", self.registry,
                                     devices=[Path(f"/dev/nbd{index}") for index in range(devices)],
                                     device_factory=Device, mount=lambda device, target: None,
                                     unmount=lambda target: None, mounted=lambda path: False,
                                     attach_concurrency=attach_concurrency, rafs=rafs)
        self.addCleanup(backend.close)
        return backend

    def other(self):
        image = self.root / "other.erofs"
        image.write_bytes(b"z" * CHUNK_BYTES)
        return self.registry.publish(image, sign_component(image, source_image="sha256:" + "2" * 64,
                                                           signing_key=self.key), tag="other")

    def test_a_slow_attach_does_not_block_another_component(self):
        backend, other = self.backend(), self.other()
        self.gates[self.digest] = threading.Event()
        with ThreadPoolExecutor(2) as pool:
            slow = pool.submit(backend.ensure, self.digest)
            started = time.monotonic()
            self.assertTrue(backend.ensure(other))
            self.assertLess(time.monotonic() - started, 2)
            self.assertFalse(slow.done())
            self.assertFalse(backend.drop(self.digest))  # Its attach still owns it.
            self.gates[self.digest].set()
            self.assertTrue(slow.result(5))
        self.assertEqual(len(self.factory_calls), 2)

    def test_the_default_attaches_one_component_at_a_time(self):
        backend, other = self.backend(attach_concurrency=1), self.other()
        self.device_gate = threading.Event()
        with ThreadPoolExecutor(2) as pool:
            slow = pool.submit(backend.ensure, self.digest)
            time.sleep(.2)
            blocked = pool.submit(backend.ensure, other)
            time.sleep(.3)
            self.assertFalse(blocked.done())  # 0.8.2's serial attach (0.8.3 burst regression)
            self.device_gate.set()
            self.assertTrue(slow.result(5) and blocked.result(5))

    def test_rafs_attaches_never_queue_behind_the_erofs_limit(self):
        # M2 wave 1: serial nydusd attaches cost a 20-sandbox burst about 30 s.
        backend, other = self.backend(attach_concurrency=1, rafs=lambda digest, component: component), self.other()
        self.device_gate = threading.Event()
        with mock.patch("ucloud_sandboxes.environment_backend.RafsEnvironmentComponent",
                        type(self.registry.load(self.digest))), ThreadPoolExecutor(2) as pool:
            slow = pool.submit(backend.ensure, self.digest)
            time.sleep(.2)
            self.assertTrue(backend.ensure(other))  # Its own RAFS slot.
            self.assertFalse(slow.done())
            self.device_gate.set()
            self.assertTrue(slow.result(5))

    def test_concurrent_callers_of_one_component_share_one_attach(self):
        backend = self.backend()
        self.gates[self.digest] = threading.Event()
        with ThreadPoolExecutor(4) as pool:
            calls = [pool.submit(backend.ensure, self.digest) for _ in range(4)]
            time.sleep(.2)
            self.gates[self.digest].set()
            self.assertEqual(len({call.result(5) for call in calls}), 1)
        self.assertEqual((len(self.loads), len(self.factory_calls)), (1, 1))

    def test_a_failed_attach_fails_its_joiners_and_the_next_call_retries(self):
        backend = self.backend()
        self.gates[self.digest] = threading.Event()
        self.registry.client.manifests.clear()
        with ThreadPoolExecutor(2) as pool:
            calls = [pool.submit(backend.ensure, self.digest) for _ in range(2)]
            time.sleep(.1)
            self.gates[self.digest].set()
            for call in calls:
                with self.assertRaises(Exception):
                    call.result(5)
        self.assertEqual(backend.metrics()["active_components"], 0)
        self.assertEqual(len(self.loads), 1)


if __name__ == "__main__":
    unittest.main()
