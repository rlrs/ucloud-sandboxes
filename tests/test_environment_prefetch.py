"""Attach-time metadata prefetch and startup-trace replay (plan C2.2, C2.3)."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import socket
from tempfile import TemporaryDirectory
from threading import Event, Thread
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from tests.test_environment_artifact import MemoryRegistry
from tests.test_erofs_metadata import LAYOUT_TWO, NEEDS_LAYOUT_TWO, build_tree, builder_image, scattered_tree
from ucloud_sandboxes.environment_artifact import (
    CHUNK_BYTES, EnvironmentArtifactRegistry, content_digest, sign_component,
)
from ucloud_sandboxes.environment_backend import (
    EnvironmentBackend, EnvironmentBackendClient, EnvironmentBackendServer, PrefetchPolicy, serve_backend,
)
from ucloud_sandboxes.environment_cache import VerifiedEnvironmentCache
from ucloud_sandboxes.environment_metadata import sign_hint, sign_metadata_hint
from ucloud_sandboxes.environment_rootfs import EnvironmentRootfsStore
from ucloud_sandboxes.environment_trace import (TRACE_ANNOTATION, LocalTraceStore, RegistryTraceStore,
                                                trace_order)
from ucloud_sandboxes.managed_registry import RegistryRequestError
from ucloud_sandboxes.models import ENVIRONMENT_IO_METRICS, NodeRuntimeMetrics, utc_now

CHUNKS = 40


class CountingRegistry(MemoryRegistry):
    """Records every range request; can hold, fail or corrupt bulk reads."""

    def __init__(self):
        super().__init__()
        self.requests = []  # (first chunk, chunk count) per HTTP range request.
        self.hold = None
        self.held = Event()
        self.failures = []  # Exceptions raised by the next range requests.
        self.corrupt = set()

    def blob_range(self, repository, digest, offset, length, *, timeout_seconds=None):
        self.requests.append((offset // CHUNK_BYTES, -(-length // CHUNK_BYTES)))
        if self.hold is not None and length > CHUNK_BYTES:
            self.held.set()
            assert self.hold.wait(5)
        if self.failures:
            raise self.failures.pop(0)
        data = bytearray(self.blobs[digest][offset:offset + length])
        for index in self.corrupt:
            if offset <= index * CHUNK_BYTES < offset + length:
                data[index * CHUNK_BYTES - offset] ^= 0xFF
        return bytes(data)


def wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("condition not reached")
        time.sleep(.005)


class PrefetchFixture(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.key = Ed25519PrivateKey.generate()
        public = self.key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        self.client = CountingRegistry()
        self.registry = EnvironmentArtifactRegistry(self.client, "environments", {content_digest(public): public})
        self.image = self.root / "image.erofs"
        self.image.write_bytes(b"".join(bytes([index + 1]) * CHUNK_BYTES for index in range(CHUNKS)))
        self.component = sign_component(self.image, source_image="sha256:" + "1" * 64, signing_key=self.key)
        self.digest = self.registry.publish(self.image, self.component, tag="fixture")
        self.registry.load(self.digest)

    def cache(self, name="cache", **kwargs):
        cache = VerifiedEnvironmentCache(self.root / name, self.registry, **kwargs)
        self.addCleanup(cache.close)
        return cache

    def cached(self, cache):
        return {index for index, chunk in enumerate(self.component.chunks)
                if (cache.root / chunk.digest[7:]).exists()}


class CachePrefetchTests(PrefetchFixture):
    def test_bulk_coalesced_ranges_install_verified_chunks(self):
        cache = self.cache()
        job = cache.prefetch(self.component, [*range(20), 30, 31], kind="metadata", max_bytes=1 << 30)
        self.assertTrue(job.wait(5))
        self.assertEqual(job.outcome, "complete")
        self.assertEqual(self.client.requests, [(0, 16), (16, 4), (30, 2)])
        self.assertEqual(self.cached(cache), {*range(20), 30, 31})
        metrics = cache.metrics()
        self.assertEqual((metrics["metadata_prefetch_chunks"], metrics["metadata_prefetch_bytes"]),
                         (22, 22 * CHUNK_BYTES))
        self.assertEqual(metrics["downloaded_bytes"], 22 * CHUNK_BYTES)
        self.assertEqual(metrics["misses"], 0)
        self.assertGreater(metrics["metadata_prefetch_seconds"], 0)
        self.assertEqual(cache.read(self.component, 17 * CHUNK_BYTES, 8), bytes([18]) * 8)
        self.assertEqual(len(self.client.requests), 3)

    def test_byte_count_and_time_budgets_bound_a_job(self):
        cache = self.cache()
        for kwargs, fetched, outcome in (({"max_bytes": 5 * CHUNK_BYTES}, 5, "budget"),
                                         ({"max_bytes": 1 << 30, "max_chunks": 3}, 3, "budget"),
                                         ({"max_bytes": 1 << 30, "deadline_seconds": 0}, 0, "deadline")):
            with self.subTest(kwargs=kwargs):
                job = cache.prefetch(self.component, range(CHUNKS), kind="trace", **kwargs)
                self.assertTrue(job.wait(5))
                self.assertEqual((job.fetched_chunks, job.outcome), (fetched, outcome))
                for chunk in self.component.chunks:
                    (cache.root / chunk.digest[7:]).unlink(missing_ok=True)
                cache._lru.clear()
                cache._bytes = 0
        self.assertEqual(cache.metrics()["trace_prefetch_truncated"], 3)
        self.assertEqual(cache.metrics()["trace_prefetch_chunks"], 8)

    def test_warm_chunks_untrusted_indices_and_duplicates_are_skipped(self):
        cache = self.cache()
        cache.read(self.component, 0, 4 * CHUNK_BYTES)
        self.client.requests.clear()
        job = cache.prefetch(self.component, [-1, CHUNKS, "7", 2, 0, 1, 2, 3, 4, 5, 5], kind="metadata",
                             max_bytes=1 << 30)
        self.assertTrue(job.wait(5))
        self.assertEqual(self.client.requests, [(4, 2)])
        self.assertEqual((job.fetched_chunks, job.skipped_chunks), (2, 4))
        job = cache.prefetch(self.component, range(6), kind="metadata", max_bytes=1 << 30)
        self.assertTrue(job.wait(5))
        self.assertEqual((job.fetched_chunks, job.skipped_chunks, job.outcome), (0, 6, "complete"))
        self.assertEqual(len(self.client.requests), 1)

    def test_failed_or_corrupt_ranges_degrade_to_demand_loading(self):
        cache = self.cache()
        self.client.failures = [RegistryRequestError(503, "GET", "/blob", "busy")]
        job = cache.prefetch(self.component, range(4), kind="metadata", max_bytes=1 << 30)
        self.assertTrue(job.wait(5))
        self.assertEqual((job.fetched_chunks, job.failed_chunks), (0, 4))
        self.assertEqual(self.client.requests, [(0, 4)])  # A prefetch never retries.
        self.assertEqual(cache.read(self.component, CHUNK_BYTES, 4), bytes([2]) * 4)
        self.client.corrupt = {6}
        job = cache.prefetch(self.component, range(4, 8), kind="metadata", max_bytes=1 << 30)
        self.assertTrue(job.wait(5))
        self.assertEqual((job.fetched_chunks, job.failed_chunks), (3, 1))
        self.assertEqual(self.cached(cache) & {4, 5, 6, 7}, {4, 5, 7})
        with self.assertRaisesRegex(ValueError, "content identity"):
            cache.read(self.component, 6 * CHUNK_BYTES, 4)  # Corrupt bytes never reach a reader.
        self.client.corrupt.clear()
        self.assertEqual(cache.read(self.component, 6 * CHUNK_BYTES, 4), bytes([7]) * 4)
        self.assertEqual(cache.metrics()["metadata_prefetch_failed_chunks"], 5)

    def test_reader_joining_a_failed_bulk_read_fetches_the_chunk_itself(self):
        cache = self.cache()
        self.client.hold = Event()
        job = cache.prefetch(self.component, range(8), kind="metadata", max_bytes=1 << 30)
        self.assertTrue(self.client.held.wait(5))
        with ThreadPoolExecutor(1) as pool:
            reader = pool.submit(cache.read, self.component, 3 * CHUNK_BYTES, 4)
            wait_until(lambda: cache.metrics()["prefetch_joined_reads"] == 1)
            self.client.failures = [RegistryRequestError(500, "GET", "/blob", "broken")]
            self.client.hold.set()
            self.assertEqual(reader.result(5), bytes([4]) * 4)
        self.assertTrue(job.wait(5))
        self.assertEqual(self.client.requests, [(0, 8), (3, 1)])
        self.assertEqual(cache.metrics()["misses"], 1)

    def test_cancellation_installs_nothing_yet_joined_readers_get_verified_bytes(self):
        cache = self.cache()
        self.client.hold = Event()
        job = cache.prefetch(self.component, range(8), kind="trace", max_bytes=1 << 30)
        self.assertTrue(self.client.held.wait(5))
        with ThreadPoolExecutor(1) as pool:
            reader = pool.submit(cache.read, self.component, 2 * CHUNK_BYTES, 4)
            wait_until(lambda: cache.metrics()["prefetch_joined_reads"] == 1)
            cache.cancel_prefetch(self.component)
            self.client.hold.set()
            self.assertEqual(reader.result(5), bytes([3]) * 4)
        self.assertTrue(job.wait(5))
        self.assertEqual((job.outcome, job.fetched_chunks), ("cancelled", 0))
        self.assertEqual(self.cached(cache), set())
        self.assertEqual(self.client.requests, [(0, 8)])

    def test_prefetch_takes_bounded_slots_and_never_starves_demand_misses(self):
        cache = self.cache(concurrent_misses=2, prefetch_slots=1)
        self.client.hold = Event()
        job = cache.prefetch(self.component, range(32), kind="trace", max_bytes=1 << 30)
        self.assertTrue(self.client.held.wait(5))
        # One bulk range holds the only prefetch slot; demand still gets one.
        started = time.monotonic()
        self.assertEqual(cache.read(self.component, 35 * CHUNK_BYTES, 4), bytes([36]) * 4)
        self.assertLess(time.monotonic() - started, 1)
        self.assertEqual(cache.metrics()["prefetch_ranges_inflight"], 1)
        with cache._guard:
            cache._demand_waiting += 1
            self.assertFalse(cache._prefetch_slot())  # A waiting demand miss goes first.
            cache._demand_waiting -= 1
        self.client.hold.set()
        self.assertTrue(job.wait(5))
        self.assertEqual(self.client.requests, [(0, 16), (35, 1), (16, 16)])

    def test_concurrent_jobs_together_schedule_at_most_half_the_cache(self):
        cache = self.cache(max_bytes=8 * CHUNK_BYTES, concurrent_misses=4, prefetch_slots=2)
        self.client.hold = Event()
        first = cache.prefetch(self.component, range(3), kind="trace", max_bytes=1 << 30)
        self.assertTrue(self.client.held.wait(5))
        second = cache.prefetch(self.component, range(10, 13), kind="trace", max_bytes=1 << 30)
        self.client.hold.set()
        self.assertTrue(first.wait(5) and second.wait(5))
        self.assertEqual((first.fetched_chunks, second.fetched_chunks, second.outcome), (3, 1, "budget"))
        third = cache.prefetch(self.component, range(20, 23), kind="trace", max_bytes=1 << 30)
        self.assertTrue(third.wait(5))
        self.assertEqual(third.fetched_chunks, 3)  # Finished jobs release their share.
        self.assertEqual(cache._prefetch_scheduled, 0)

    def test_a_replay_holding_the_shared_share_never_starves_later_metadata(self):
        cache = self.cache(max_bytes=16 * CHUNK_BYTES, concurrent_misses=8, prefetch_slots=2)
        self.client.hold = Event()
        trace = cache.prefetch(self.component, range(8), kind="trace", max_bytes=1 << 30)
        self.assertTrue(self.client.held.wait(5))  # Half the cache, unfinished.
        # Single-chunk ranges, which the fixture does not hold.
        metadata = cache.prefetch(self.component, [20, 22, 24, 26], kind="metadata", max_bytes=1 << 30)
        self.assertTrue(metadata.wait(5))
        self.assertEqual((metadata.outcome, metadata.fetched_chunks), ("complete", 4))
        # Replays still respect the share the unfinished one holds.
        later = cache.prefetch(self.component, [30], kind="trace", max_bytes=1 << 30)
        self.assertTrue(later.wait(5))
        self.assertEqual((later.outcome, later.fetched_chunks), ("budget", 0))
        self.client.hold.set()
        self.assertTrue(trace.wait(5))
        self.assertEqual((trace.outcome, trace.fetched_chunks), ("complete", 8))

    def test_joined_reader_and_its_fallback_share_one_fetch_budget(self):
        # The budget matches the NBD request timeout: a stalled bulk range
        # must not hold a joined reader past it.
        cache = self.cache(fetch_timeout_seconds=1.0)
        self.client.hold = Event()
        job = cache.prefetch(self.component, range(8), kind="metadata", max_bytes=1 << 30)
        self.assertTrue(self.client.held.wait(5))
        started = time.monotonic()
        self.assertEqual(cache.read(self.component, 3 * CHUNK_BYTES, 4), bytes([4]) * 4)
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertEqual(cache.metrics()["prefetch_joined_reads"], 1)
        self.client.failures = [RegistryRequestError(500, "GET", "/blob", "stalled")]
        self.client.hold.set()
        self.assertTrue(job.wait(5))
        self.assertEqual(self.client.requests, [(0, 8), (3, 1)])

    def test_metadata_jobs_run_before_trace_jobs(self):
        cache = self.cache(concurrent_misses=4, prefetch_slots=1)
        self.client.hold = Event()
        first = cache.prefetch(self.component, range(16), kind="trace", max_bytes=1 << 30)
        self.assertTrue(self.client.held.wait(5))
        trace = cache.prefetch(self.component, range(20, 36), kind="trace", max_bytes=1 << 30)
        metadata = cache.prefetch(self.component, range(16, 20), kind="metadata", max_bytes=1 << 30)
        self.client.hold.set()
        for job in (first, trace, metadata):
            self.assertTrue(job.wait(5))
        self.assertEqual(self.client.requests, [(0, 16), (16, 4), (20, 16)])

    def test_per_chunk_publications_prefetch_one_blob_per_chunk(self):
        for chunk, offset in zip(self.component.chunks, range(0, CHUNKS * CHUNK_BYTES, CHUNK_BYTES)):
            self.client.blobs[chunk.digest] = self.image.read_bytes()[offset:offset + chunk.size]
        with patch.object(self.registry, "whole_image", return_value=False):
            cache = self.cache()
            self.client.reads.clear()
            job = cache.prefetch(self.component, range(3), kind="metadata", max_bytes=1 << 30)
            self.assertTrue(job.wait(5))
        self.assertEqual(self.client.reads, [chunk.digest for chunk in self.component.chunks[:3]])
        self.assertEqual(self.client.requests, [])

    def test_close_cancels_queued_and_running_jobs(self):
        cache = VerifiedEnvironmentCache(self.root / "closing", self.registry)
        self.client.hold = Event()
        job = cache.prefetch(self.component, range(32), kind="trace", max_bytes=1 << 30)
        self.assertTrue(self.client.held.wait(5))
        self.client.hold.set()
        cache.close()
        self.assertTrue(job.done.is_set())
        self.assertFalse(cache._background.is_alive())
        late = cache.prefetch(self.component, range(2), kind="trace", max_bytes=1 << 30)
        self.assertEqual((late.outcome, late.done.is_set()), ("cancelled", True))


class StartupTraceTests(PrefetchFixture):
    def test_window_records_first_reads_in_order_up_to_its_count(self):
        cache = self.cache()
        saved, done = [], Event()
        sink = lambda component, chunks: (saved.append((component, chunks)), done.set())  # noqa: E731
        self.assertTrue(cache.record_startup(self.component, sink, window_seconds=30, max_chunks=4))
        self.assertFalse(cache.record_startup(self.component, sink))
        for index in (5, 2, 5, 9, 1, 7, 3):
            cache.read(self.component, index * CHUNK_BYTES + 10, 4)
        self.assertTrue(done.wait(5))
        self.assertEqual(saved, [(self.component, (5, 2, 9, 1))])
        metrics = cache.metrics()
        self.assertEqual((metrics["traces_recorded"], metrics["trace_chunks_recorded"]), (1, 4))

    def test_window_closes_by_time_and_empty_or_stopped_windows_save_nothing(self):
        cache = self.cache()
        saved, done = [], Event()
        cache.record_startup(self.component, lambda c, chunks: (saved.append(chunks), done.set()),
                             window_seconds=.1, max_chunks=2048)
        cache.read(self.component, 3 * CHUNK_BYTES, 2 * CHUNK_BYTES + 1)
        self.assertTrue(done.wait(5))
        self.assertEqual(saved, [(3, 4, 5)])
        empty = []
        cache.record_startup(self.component, lambda c, chunks: empty.append(chunks), window_seconds=.05)
        time.sleep(.2)
        cache.record_startup(self.component, lambda c, chunks: empty.append(chunks), window_seconds=.05)
        cache.read(self.component, 0, 1)
        cache.stop_recording(self.component)
        time.sleep(.2)
        self.assertEqual(empty, [])
        cache.read(self.component, 0, 1)
        self.assertEqual(cache.metrics()["traces_recorded"], 1)

    def test_trace_order_coalesces_runs_and_keeps_first_touch_first(self):
        self.assertEqual(trace_order([9, 3, 4, 10, 1, 5, 3]), (9, 10, 3, 4, 5, 1))
        self.assertEqual(trace_order([]), ())

    def test_local_store_is_atomic_bounded_and_treats_content_as_untrusted(self):
        store = LocalTraceStore(self.root / "traces")
        self.assertEqual(store.load(self.component), ("absent", None))
        store.save(self.component, [4, 2, 4, 9])
        self.assertEqual(store.load(self.component), ("present", (4, 2, 9)))
        self.assertEqual([path.name for path in store.root.iterdir()], [self.component.image_digest[7:] + ".json"])
        path = store.root / (self.component.image_digest[7:] + ".json")
        good = json.loads(path.read_bytes())
        for name, payload in (("garbage", b"{"), ("list", b"[]"), ("deep", b"[" * 50000),
                              ("schema", json.dumps(good | {"schema": "v0"}).encode()),
                              ("other image", json.dumps(good | {"image_digest": "sha256:" + "7" * 64}).encode()),
                              ("index range", json.dumps(good | {"chunks": [CHUNKS]}).encode()),
                              ("duplicate", json.dumps(good | {"chunks": [1, 1]}).encode()),
                              ("chunk count", json.dumps(good | {"chunk_count": 3}).encode()),
                              ("oversized", b" " * (65 * 1024) + json.dumps(good).encode())):
            with self.subTest(name=name):
                path.write_bytes(payload)
                with self.assertLogs("ucloud_sandboxes.environment_trace", "WARNING"):
                    self.assertEqual(store.load(self.component), ("invalid", None))
                self.assertFalse(path.exists())
        os.symlink(self.image, path)
        self.assertEqual(store.load(self.component), ("invalid", None))
        path.unlink()
        store.save(self.component, [])
        self.assertEqual(store.load(self.component), ("absent", None))

    def test_local_store_keeps_only_the_newest_traces(self):
        store = LocalTraceStore(self.root / "bounded", max_traces=2)
        components = []
        for index in range(3):
            image = self.root / f"trace-{index}.erofs"
            image.write_bytes(bytes([index]) * CHUNK_BYTES)
            components.append(sign_component(image, source_image="sha256:" + "6" * 64, signing_key=self.key))
            store.save(components[-1], [0])
            # File times are coarse; order the saves explicitly.
            os.utime(store.root / (components[-1].image_digest[7:] + ".json"), ns=(index + 1, index + 1))
        self.assertEqual([store.load(component)[0] for component in components], ["absent", "present", "present"])
        self.assertEqual(len(list(store.root.iterdir())), 2)
        with self.assertRaises(ValueError):
            LocalTraceStore(self.root / "unbounded", max_traces=0)


class BackendPrefetchTests(PrefetchFixture):
    def backend(self, name="backend", policy=PrefetchPolicy(), traces=None, **kwargs):
        mounts = set()
        self.mounts = []

        class Device:
            healthy = True

            def __init__(device, path, *args, **kwargs):
                device.path = path

            def close(device):
                pass

        def mount(device, target):
            self.mounts.append(target)
            mounts.add(target)

        backend = EnvironmentBackend(self.root / name, self.registry, devices=[Path("/dev/nbd-test")],
                                     device_factory=Device, mount=mount, unmount=mounts.discard,
                                     mounted=lambda path: path in mounts, referenced=lambda _: False,
                                     prefetch=policy, traces=traces, **kwargs)
        self.addCleanup(backend.close)
        return backend

    def publish_hinted(self, chunks):
        hint = sign_hint(self.component, chunks, self.key)
        digest = self.registry.publish(self.image, self.component, tag="hinted", metadata=hint)
        self.registry.load(digest)
        return digest

    def test_metadata_hint_is_fetched_before_ensure_reports_ready(self):
        digest = self.publish_hinted([(0, 4096), (1, 200), (2, 9000), (12, 100), (13, 100), (30, 5)])
        backend = self.backend()
        backend.ensure(digest)
        self.assertEqual(self.cached(backend.cache), {0, 1, 2, 12, 13, 30})
        self.assertEqual(self.client.requests, [(0, 3), (12, 2), (30, 1)])
        metrics = backend.metrics()
        self.assertEqual((metrics["metadata_hint_present"], metrics["metadata_prefetch_chunks"]), (1, 6))
        self.assertEqual(metrics["metadata_prefetch_wait_timeouts"], 0)
        self.assertEqual(len(self.mounts), 1)
        # A second ensure of the live component neither waits nor refetches.
        backend.ensure(digest)
        self.assertEqual(len(self.client.requests), 3)

    def test_metadata_budget_takes_the_densest_chunks(self):
        digest = self.publish_hinted([(0, 4096), (1, 200), (2, 9000), (12, 100), (13, 100), (30, 5)])
        backend = self.backend(policy=PrefetchPolicy(metadata_bytes=3 * CHUNK_BYTES))
        backend.ensure(digest)
        self.assertEqual(self.cached(backend.cache), {0, 1, 2})
        self.assertEqual(backend.metrics()["metadata_prefetch_truncated"], 0)

    def test_absent_hint_and_prefetch_failures_never_fail_attach(self):
        backend = self.backend()
        backend.ensure(self.digest)
        self.assertEqual(backend.metrics()["metadata_hint_absent"], 1)
        self.assertEqual(self.client.requests, [])
        backend.drop(self.digest)
        digest = self.publish_hinted([(0, 4096), (1, 100)])
        self.client.failures = [RegistryRequestError(503, "GET", "/blob", "busy")]
        other = self.backend("failing")
        other.ensure(digest)
        self.assertEqual(other.metrics()["metadata_prefetch_failed_chunks"], 2)
        other.drop(digest)
        with patch.object(other.cache, "prefetch", side_effect=RuntimeError("scheduler broken")), \
             self.assertLogs("ucloud_sandboxes.environment_backend", "WARNING"):
            other.ensure(digest)
        self.assertEqual(other.metrics()["prefetch_start_failures"], 1)

    def test_ready_wait_is_bounded_and_detach_cancels_prefetch(self):
        digest = self.publish_hinted([(index, 100) for index in range(16)])
        backend = self.backend(policy=PrefetchPolicy(metadata_wait_seconds=.05))
        self.client.hold = Event()
        started = time.monotonic()
        backend.ensure(digest)
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(backend.metrics()["metadata_prefetch_wait_timeouts"], 1)
        self.assertTrue(backend.drop(digest))
        self.client.hold.set()
        wait_until(lambda: backend.metrics()["prefetch_jobs_active"] == 0)
        self.assertEqual(self.cached(backend.cache), set())

    def test_a_metadata_wait_never_blocks_other_rpcs_yet_every_caller_waits_for_ready(self):
        backend = self.backend()
        backend.ensure(self.digest)  # A live composition's component.
        digest = self.publish_hinted([(index, 100) for index in range(16)])
        endpoint = self.root / "backend.sock"
        server = EnvironmentBackendServer(endpoint, backend)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (server.shutdown(), thread.join(), server.server_close()))
        self.client.hold = Event()
        with ThreadPoolExecutor(2) as pool:
            attaching = pool.submit(EnvironmentBackendClient(endpoint).ensure, digest)
            self.assertTrue(self.client.held.wait(5))
            # Liveness of another component answers while the attach waits.
            started = time.monotonic()
            self.assertEqual(EnvironmentBackendClient(endpoint).ensure(self.digest),
                             backend.mounts / self.digest[7:])
            self.assertLess(time.monotonic() - started, 1)
            # A second caller of the warming component waits like the first.
            joining = pool.submit(EnvironmentBackendClient(endpoint).ensure, digest)
            time.sleep(.2)
            self.assertFalse(attaching.done() or joining.done())
            self.client.hold.set()
            self.assertEqual(attaching.result(5), joining.result(5))
        self.assertEqual(self.cached(backend.cache), set(range(16)))
        self.assertEqual(backend.metrics()["metadata_prefetch_wait_timeouts"], 0)

    def test_first_attach_records_a_trace_and_a_later_attach_replays_it(self):
        traces = LocalTraceStore(self.root / "shared-traces")
        policy = PrefetchPolicy(trace_window_seconds=.1)
        first = self.backend("first", policy=policy, traces=traces)
        first.ensure(self.digest)
        self.assertEqual(first.metrics()["trace_recordings_started"], 1)
        for index in (7, 8, 9, 20, 3):  # The guest's first reads after attach.
            first.cache.read(self.component, index * CHUNK_BYTES, 512)
        wait_until(lambda: traces.load(self.component)[0] == "present")
        self.assertEqual(traces.load(self.component), ("present", (7, 8, 9, 20, 3)))
        first.drop(self.digest)
        self.client.requests.clear()
        later = self.backend("later", policy=policy, traces=traces)
        later.ensure(self.digest)
        wait_until(lambda: later.metrics()["trace_prefetch_chunks"] == 5)
        self.assertEqual(self.client.requests, [(7, 3), (20, 1), (3, 1)])
        metrics = later.metrics()
        self.assertEqual((metrics["trace_hint_present"], metrics["trace_recordings_started"]), (1, 0))
        self.assertEqual(metrics["trace_prefetch_bytes"], 5 * CHUNK_BYTES)
        for index in (7, 8, 9, 20, 3):
            later.cache.read(self.component, index * CHUNK_BYTES, 512)
        self.assertEqual(len(self.client.requests), 3)
        self.assertEqual(later.metrics()["misses"], 0)

    def test_a_fresh_node_replays_a_trace_another_node_shared(self):
        # Plan C2.7: traces travel through the managed registry, so a node that
        # never attached the component starts in traced mode.
        policy = PrefetchPolicy(trace_window_seconds=.1)
        shared = RegistryTraceStore(LocalTraceStore(self.root / "node-a-traces"), self.client)
        first = self.backend("first", policy=policy, traces=shared)
        first.ensure(self.digest)
        for index in (7, 8, 9, 20, 3):
            first.cache.read(self.component, index * CHUNK_BYTES, 512)
        wait_until(lambda: "trace-" + self.component.image_digest[7:] in self.client.tags)
        first.drop(self.digest)
        self.client.requests.clear()
        fresh = RegistryTraceStore(LocalTraceStore(self.root / "node-b-traces"), self.client)
        later = self.backend("later", policy=policy, traces=fresh)
        later.ensure(self.digest)
        wait_until(lambda: later.metrics()["trace_prefetch_chunks"] == 5)
        self.assertEqual(self.client.requests, [(7, 3), (20, 1), (3, 1)])
        self.assertEqual((later.metrics()["trace_hint_present"], later.metrics()["trace_recordings_started"]), (1, 0))
        self.assertEqual(fresh.local.load(self.component), ("present", (7, 8, 9, 20, 3)))  # Kept locally.

    def test_a_shared_trace_is_untrusted_bounded_and_optional(self):
        calls = []
        real = self.client.manifest_document

        def counting(*args, **kwargs):
            calls.append(kwargs.get("timeout_seconds"))
            return real(*args, **kwargs)
        store = RegistryTraceStore(LocalTraceStore(self.root / "traces"), self.client, timeout_seconds=1.5)
        with patch.object(self.client, "manifest_document", side_effect=counting):
            self.assertEqual(store.load(self.component), ("absent", None))  # Nobody shared one.
            self.assertEqual(calls, [1.5])
            tag = store.tag(self.component)
            for bad in ("not json", json.dumps({"schema": "x"}), json.dumps({
                    "schema": "ucloud-environment-startup-trace-v1", "image_digest": self.component.image_digest,
                    "chunk_count": len(self.component.chunks), "chunks": [CHUNKS + 5]})):
                self.client.put_manifest("environment-traces", tag, json.dumps(
                    {"schemaVersion": 2, "annotations": {TRACE_ANNOTATION: bad}}).encode(), media_type="x")
                self.assertEqual(store.load(self.component)[0], "invalid")
                self.assertEqual(store.local.load(self.component), ("absent", None))  # Never kept.
            store.local.save(self.component, [4, 2])
            calls.clear()
            self.assertEqual(store.load(self.component), ("present", (4, 2)))
            self.assertEqual(calls, [])  # The node's own trace wins, with no request.
        with patch.object(self.client, "manifest_document", side_effect=OSError("registry down")):
            store.local.save(self.component, [])
            (store.local.root / (self.component.image_digest[7:] + ".json")).unlink()
            self.assertEqual(store.load(self.component), ("absent", None))
        store.close()

    def test_a_detach_inside_the_window_saves_what_the_guest_read(self):
        # Production: a sandbox is created, used and deleted inside the default
        # 30 s window, and delete's image collection drops each component.
        traces = LocalTraceStore(self.root / "traces")
        backend = self.backend(traces=traces)
        backend.ensure(self.digest)
        component = backend._components[self.digest]  # What the NBD export reads.
        for index in (4, 5, 30):
            backend.cache.read(component, index * CHUNK_BYTES, 512)
        self.assertTrue(backend.drop(self.digest))
        metrics = backend.metrics()
        self.assertEqual((metrics["trace_recordings_started"], metrics["traces_recorded"],
                          metrics["trace_chunks_recorded"]), (1, 1, 3))
        self.assertEqual(traces.load(self.component), ("present", (4, 5, 30)))
        backend.ensure(self.digest)  # The next attach replays it.
        self.assertEqual(backend.metrics()["trace_hint_present"], 1)

    def test_a_failed_mount_or_unread_detach_saves_no_trace(self):
        traces = LocalTraceStore(self.root / "traces")
        backend = self.backend(traces=traces)

        def failing(device, target):
            backend.cache.read(backend._components[self.digest], 0, 512)  # The superblock read.
            raise OSError("mount failed")
        backend._mount = failing
        with self.assertRaises(OSError):
            backend.ensure(self.digest)
        backend._mount = lambda device, target: self.mounts.append(target)
        backend.ensure(self.digest)
        self.assertTrue(backend.drop(self.digest))
        self.assertEqual(traces.load(self.component), ("absent", None))
        self.assertEqual(backend.metrics()["trace_recordings_started"], 2)
        self.assertEqual(backend.metrics()["traces_recorded"], 0)

    def test_trace_budget_is_capped_by_the_cache(self):
        traces = LocalTraceStore(self.root / "traces")
        traces.save(self.component, range(CHUNKS))
        backend = self.backend(traces=traces, cache_bytes=8 * CHUNK_BYTES)
        backend.ensure(self.digest)
        wait_until(lambda: backend.metrics()["prefetch_jobs_active"] == 0)
        self.assertEqual(backend.metrics()["trace_prefetch_chunks"], 2)  # A quarter of the cache.

    def test_disabled_policy_attaches_without_hints_or_traces(self):
        digest = self.publish_hinted([(0, 4096)])
        backend = self.backend(policy=PrefetchPolicy(enabled=False))
        backend.ensure(digest)
        self.assertEqual(self.client.requests, [])
        self.assertEqual(backend.metrics()["trace_recordings_started"], 0)
        self.assertIs(backend.metrics()["prefetch_enabled"], False)

    def test_service_command_passes_the_off_switch_to_the_backend(self):
        from ucloud_sandboxes import cli, environment_backend
        common = ["serve-environment-io", "--root", "/state/environment-io", "--socket", "/run/io.sock"]
        for argv, prefetch in ((common, True), ([*common, "--disable-prefetch"], False)):
            with self.subTest(prefetch=prefetch), patch.object(environment_backend, "serve_backend") as serve:
                args = cli.build_parser().parse_args(argv)
                args.func(args)
                self.assertIs(serve.call_args.kwargs["prefetch"], prefetch)
        with patch.object(environment_backend.os, "geteuid", return_value=0), \
             patch.object(environment_backend, "EnvironmentBackend") as backend, \
             patch.object(environment_backend, "EnvironmentBackendServer"):
            serve_backend(object(), root=self.root, socket_path=self.root / "io.sock", prefetch=False)
        self.assertEqual(backend.call_args.kwargs["prefetch"], PrefetchPolicy(enabled=False))

    def test_heartbeats_read_metrics_over_rpc_while_an_attach_holds_the_backend(self):
        backend = self.backend()
        self.assertEqual(set(backend.metrics()), ENVIRONMENT_IO_METRICS)
        backend.ensure(self.publish_hinted([(0, 4096), (1, 200)]))
        endpoint = self.root / "backend.sock"
        server = EnvironmentBackendServer(endpoint, backend)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (server.shutdown(), thread.join(), server.server_close()))
        store = EnvironmentRootfsStore(self.root / "store", self.registry, EnvironmentBackendClient(endpoint),
                                       block_devices=1)
        with backend._guard:  # As during a registry load or mount.
            exported = store.io_metrics()
        self.assertEqual(exported, backend.metrics())
        self.assertEqual((exported["metadata_hint_present"], exported["metadata_prefetch_chunks"],
                          exported["active_components"], exported["prefetch_enabled"]), (1, 2, 1, True))
        heartbeat = NodeRuntimeMetrics(collected_at=utc_now(), environment_io=exported)
        self.assertEqual(NodeRuntimeMetrics.from_dict(json.loads(json.dumps(heartbeat.to_dict()))), heartbeat)
        for request in ({"method": "metrics", "digest": self.digest}, {"method": "ensure"}, {"method": ["drop"]}):
            with self.subTest(request=request), self.assertRaisesRegex(RuntimeError, "invalid"):
                EnvironmentBackendClient(endpoint)._call(request)

        def rejected(*_args):
            raise RuntimeError("invalid environment backend request")
        # The backend outlives agent upgrades: an older one rejects the method.
        for reply in (rejected, lambda: exported | {"invented": 0},
                      lambda: exported | {"metadata_prefetch_seconds": 0}):
            store.backend = SimpleNamespace(metrics=reply)
            self.assertIsNone(store.io_metrics())
        # A stalled backend (accepted, never answered) costs a heartbeat well
        # under the gateway's 2 s wake heartbeat read.
        stalled = socket.socket(socket.AF_UNIX)
        self.addCleanup(stalled.close)
        stalled.bind(str(self.root / "stalled.sock"))
        stalled.listen()
        store.backend, started = EnvironmentBackendClient(self.root / "stalled.sock"), time.monotonic()
        self.assertIsNone(store.io_metrics())
        self.assertLess(time.monotonic() - started, 1.5)

    def test_heartbeat_schema_is_strict_and_older_workers_report_none(self):
        exported = self.backend().metrics()
        wire = NodeRuntimeMetrics(collected_at=utc_now(), environment_io=exported).to_dict()
        # Every rejection below is then attributable to its one change.
        self.assertEqual(NodeRuntimeMetrics.from_dict(wire).environment_io, exported)
        legacy = dict(wire)
        del legacy["environment_io"]
        self.assertIsNone(NodeRuntimeMetrics.from_dict(legacy).environment_io)
        for change in ({"hits": True}, {"hits": -1}, {"misses": 1.5}, {"trace_prefetch_seconds": 1},
                       {"trace_prefetch_seconds": float("inf")}, {"prefetch_enabled": 1}, {"invented": 0}):
            with self.subTest(change=change):
                self.assertIsNone(NodeRuntimeMetrics.from_dict(wire | {"environment_io": exported | change}))
        missing = dict(exported)
        del missing["active_components"]
        self.assertIsNone(NodeRuntimeMetrics.from_dict(wire | {"environment_io": missing}))


@unittest.skipUnless(shutil.which("mkfs.erofs"), "mkfs.erofs (erofs-utils) is not installed")
class RealImagePrefetchTests(PrefetchFixture):
    def test_after_attach_every_metadata_read_is_served_locally(self):
        """The C2.2 gate in the Python worker path: no remote metadata reads."""
        build_tree(self.root / "view", small_files=40, big_entries=120)
        image = builder_image(self.root / "view", self.root / "real.erofs")
        component = sign_component(image, source_image="sha256:" + "5" * 64, signing_key=self.key)
        hint, metadata = sign_metadata_hint(image, component, self.key)
        digest = self.registry.publish(image, component, tag="real", metadata=hint)
        backend = BackendPrefetchTests.backend(self, "real", policy=PrefetchPolicy(trace_window_seconds=3600))
        backend.ensure(digest)
        fetched = len(self.client.requests)
        self.assertEqual(backend.metrics()["metadata_prefetch_chunks"], len(hint.chunks))
        for start, end in metadata.ranges:
            for offset in range(start, end, 1 << 20):
                backend.cache.read(component, offset, min(end, offset + (1 << 20)) - offset)
        self.assertEqual(len(self.client.requests), fetched)
        self.assertEqual(backend.metrics()["misses"], 0)

    @unittest.skipUnless(LAYOUT_TWO, NEEDS_LAYOUT_TWO)
    def test_layout_two_metadata_fits_the_attach_budget_that_layout_one_overflows(self):
        """C2.12: --MZ packs metadata into one zone, so attach prefetches all of it."""
        scattered_tree(self.root / "view")
        budget = PrefetchPolicy().metadata_bytes // CHUNK_BYTES
        seen = {}
        for layout in (1, 2):
            image = builder_image(self.root / "view", self.root / f"layout-{layout}.erofs",
                                  preserve_mtimes=layout == 2)
            component = sign_component(image, source_image="sha256:" + "6" * 64, signing_key=self.key)
            hint, metadata = sign_metadata_hint(image, component, self.key)
            self.assertGreater(len(component.chunks), budget)
            digest = self.registry.publish(image, component, tag=f"layout-{layout}", metadata=hint)
            backend = BackendPrefetchTests.backend(self, f"layout-{layout}",
                                                   policy=PrefetchPolicy(trace_window_seconds=3600))
            backend.ensure(digest)
            fetched = len(self.client.requests)
            for start, end in metadata.ranges:
                for offset in range(start, end, 1 << 20):
                    backend.cache.read(component, offset, min(end, offset + (1 << 20)) - offset)
            seen[layout] = (len(hint.chunks), -(-metadata.metadata_bytes // CHUNK_BYTES),
                            backend.metrics()["metadata_prefetch_chunks"], len(self.client.requests) > fetched)
        (one, _, one_prefetched, one_remote), (two, two_least, two_prefetched, two_remote) = seen[1], seen[2]
        self.assertGreater(one, budget, "fixture no longer scatters layout-1 metadata")
        self.assertEqual((one_prefetched, one_remote), (budget, True))
        # Chunk 0 (superblock, shared xattrs) and the contiguous zone.
        self.assertLessEqual(two, 2 + two_least)
        self.assertEqual((two_prefetched, two_remote), (two, False))


if __name__ == "__main__":
    unittest.main()
