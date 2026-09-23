from concurrent.futures import CancelledError, ThreadPoolExecutor
from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import time

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from ucloud_sandboxes.environment_artifact import (
    CHUNK_BYTES, EnvironmentArtifactRegistry, content_digest, sign_component,
)
from ucloud_sandboxes.environment_cache import VerifiedEnvironmentCache
from ucloud_sandboxes.managed_registry import RegistryClient, RegistryRequestError


class MemoryRegistry:
    def __init__(self):
        self.blobs, self.manifests, self.uploads = {}, {}, {}
        self.reads = []
        self.gate = None
        self.entered = Event()

    def blob_exists(self, repository, digest):
        return digest in self.blobs

    def start_blob_upload(self, repository):
        token = str(len(self.uploads))
        self.uploads[token] = b""
        return token

    def upload_blob_chunk(self, location, chunk):
        self.uploads[location] += chunk
        return location

    def finish_blob_upload(self, location, digest):
        self.blobs[digest] = self.uploads.pop(location)

    def abort_blob_upload(self, location):
        self.uploads.pop(location, None)

    def put_manifest(self, repository, tag, payload, *, media_type):
        self.manifests[content_digest(payload)] = payload

    def manifest_document(self, repository, digest):
        return json.loads(self.manifests[digest]), {}

    def blob_bytes(self, repository, digest, *, max_bytes, timeout_seconds=None):
        self.reads.append(digest)
        self.entered.set()
        if self.gate is not None:
            self.gate.wait(3)
        return self.blobs[digest][:max_bytes + 1]


class EnvironmentArtifactTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.key = Ed25519PrivateKey.generate()
        public = self.key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        self.client = MemoryRegistry()
        self.registry = EnvironmentArtifactRegistry(self.client, "environments", {content_digest(public): public})
        self.image = self.root / "image.erofs"
        self.bytes = b"a" * CHUNK_BYTES + b"b" * CHUNK_BYTES + b"c" * 4096
        self.image.write_bytes(self.bytes)
        self.component = sign_component(self.image, source_image="sha256:" + "1" * 64, signing_key=self.key)
        self.digest = self.registry.publish(self.image, self.component, tag="fixture")

    def cache(self, **kwargs):
        result = VerifiedEnvironmentCache(self.root / "cache", self.registry, **kwargs)
        self.addCleanup(result.close)
        return result

    def test_signed_root_roundtrip_and_untrusted_or_changed_index_rejected(self):
        self.assertEqual(self.registry.load(self.digest), self.component)
        with self.assertRaisesRegex(ValueError, "not trusted"):
            EnvironmentArtifactRegistry(self.client, "environments", {}).load(self.digest)
        changed = replace(self.component, source_image="sha256:" + "2" * 64)
        with self.assertRaisesRegex(ValueError, "signature"):
            changed.authenticate(self.registry.trusted_keys)
        self.image.write_bytes(b"x" * len(self.bytes))
        with self.assertRaisesRegex(ValueError, "changed after signing"):
            self.registry.publish(self.image, self.component, tag="changed")
        self.assertEqual(len(self.client.manifests), 1)

    def test_only_demanded_chunks_download_and_corruption_never_reaches_reader(self):
        cache = self.cache(max_bytes=CHUNK_BYTES)
        self.client.reads.clear()
        self.assertEqual(cache.read(self.component, CHUNK_BYTES - 5, 10), b"a" * 5 + b"b" * 5)
        self.assertEqual(self.client.reads, [c.digest for c in self.component.chunks[:2]])
        self.assertLessEqual(cache.metrics()["cached_bytes"], CHUNK_BYTES)
        chunk = self.component.chunks[1]
        target = cache.root / chunk.digest.removeprefix("sha256:")
        target.chmod(0o600)
        target.write_bytes(b"z" * chunk.size)
        self.assertEqual(cache.read(self.component, CHUNK_BYTES, 4), b"bbbb")
        self.assertEqual(cache.metrics()["corruptions"], 1)
        self.client.blobs[self.component.chunks[2].digest] = b"x" * 4096
        with self.assertRaisesRegex(ValueError, "content identity"):
            cache.read(self.component, 2 * CHUNK_BYTES, 4)
        with self.assertRaises(ValueError):
            cache.read(self.component, len(self.bytes) - 1, 2)

    def test_shared_miss_survives_one_reader_cancellation(self):
        cache = self.cache(concurrent_misses=1)
        self.client.reads.clear()
        self.client.entered.clear()
        self.client.gate = Event()
        cancelled = Event()
        with ThreadPoolExecutor(2) as pool:
            first = pool.submit(cache.read, self.component, 0, 16, cancel=cancelled)
            self.assertTrue(self.client.entered.wait(1))
            second = pool.submit(cache.read, self.component, 0, 16)
            cancelled.set()
            with self.assertRaises(CancelledError):
                first.result(1)
            self.client.gate.set()
            self.assertEqual(second.result(1), b"a" * 16)
        self.assertEqual(self.client.reads, [self.component.chunks[0].digest])
        self.assertEqual(cache.metrics()["pending_misses"], 0)

    def test_network_timeout_propagates_instead_of_waiting_on_completed_future(self):
        cache = self.cache(fetch_timeout_seconds=.01)
        def fail(*_args, **_kwargs):
            raise TimeoutError("transport deadline")
        cache.registry = SimpleNamespace(repository="environments", client=SimpleNamespace(blob_bytes=fail))
        with self.assertRaisesRegex(TimeoutError, "fetch deadline"):
            cache.read(self.component, 0, 4)


class EnvironmentReadRetryTests(EnvironmentArtifactTests):
    def test_cancel_after_temporary_write_prevents_cache_install(self):
        cache = self.cache()
        cancelled = Event()
        original = Path.chmod

        def after_write(path, mode, **kwargs):
            original(path, mode, **kwargs)
            if path.name.startswith(".chunk-"):
                cancelled.set()

        with patch.object(Path, "chmod", after_write), self.assertRaises(CancelledError):
            cache._fetch(self.component.chunks[0], cancelled)
        self.assertEqual(cache.metrics()["cached_bytes"], 0)
        self.assertEqual(list(cache.root.iterdir()), [])

    def test_slow_trickle_body_expires_without_cache_install_or_retry(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from threading import Thread
        attempts = []
        class Handler(BaseHTTPRequestHandler):
            def do_GET(inner):
                attempts.append(inner.path)
                inner.send_response(200)
                inner.send_header("Content-Length", str(CHUNK_BYTES))
                inner.end_headers()
                try:
                    for _ in range(100):
                        inner.wfile.write(b"a")
                        inner.wfile.flush()
                        time.sleep(.015)
                except (BrokenPipeError, ConnectionResetError):
                    pass
            def log_message(self, *_args):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        cache = self.cache(fetch_timeout_seconds=.08)
        cache.registry = SimpleNamespace(repository="environments",
            client=RegistryClient(f"http://127.0.0.1:{server.server_port}"))
        started = time.monotonic()
        with self.assertRaisesRegex(TimeoutError, "fetch deadline"):
            cache.read(self.component, 0, 4)
        self.assertLess(time.monotonic() - started, .4)
        self.assertEqual(len(attempts), 1)
        self.assertEqual(cache.metrics()["cached_bytes"], 0)
        self.assertEqual(cache.metrics()["fetch_retries"], 0)

    def test_immutable_read_retries_transient_http_and_disconnect_under_one_budget(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from threading import Thread
        import socket
        attempts = []
        payload = self.bytes[:CHUNK_BYTES]
        class Handler(BaseHTTPRequestHandler):
            def do_GET(inner):
                attempts.append(inner.path)
                if len(attempts) == 1:
                    inner.send_response(503)
                    inner.send_header("Content-Length", "0")
                    inner.end_headers()
                elif len(attempts) == 2:
                    inner.connection.shutdown(socket.SHUT_RDWR)
                    inner.connection.close()
                else:
                    inner.send_response(200)
                    inner.send_header("Content-Length", str(len(payload)))
                    inner.end_headers()
                    inner.wfile.write(payload)
            def log_message(self, *_args):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        cache = self.cache(fetch_timeout_seconds=2)
        cache.registry = SimpleNamespace(repository="environments",
            client=RegistryClient(f"http://127.0.0.1:{server.server_port}"))
        self.assertEqual(cache.read(self.component, 0, 4), b"aaaa")
        self.assertEqual(len(attempts), 3)
        self.assertEqual(cache.metrics()["fetch_retries"], 2)
        self.assertEqual(cache.metrics()["downloaded_bytes"], CHUNK_BYTES)

    def test_corruption_and_permanent_errors_never_retry(self):
        from ssl import SSLCertVerificationError
        from urllib.error import URLError
        cache = self.cache()
        for failure in (RegistryRequestError(404, "GET", "/blob", "missing"),
                        RegistryRequestError(403, "GET", "/blob", "forbidden")):
            with self.subTest(failure=failure), patch.object(self.client, "blob_bytes", side_effect=failure) as read:
                with self.assertRaises(RegistryRequestError):
                    cache.read(self.component, 0, 4)
                self.assertEqual(read.call_count, 1)
        with patch.object(self.client, "blob_bytes", return_value=b"x" * CHUNK_BYTES) as read:
            with self.assertRaisesRegex(ValueError, "content identity"):
                cache.read(self.component, 0, 4)
            self.assertEqual(read.call_count, 1)
        self.assertEqual(cache.metrics()["fetch_retries"], 0)
        self.assertEqual(cache.metrics()["cached_bytes"], 0)
        with patch.object(self.client, "blob_bytes",
                          side_effect=URLError(SSLCertVerificationError("untrusted"))) as read:
            with self.assertRaises(URLError):
                cache.read(self.component, 0, 4)
            self.assertEqual(read.call_count, 1)

    def test_last_reader_cancellation_stops_retries(self):
        cache = self.cache()
        gate, entered, cancelled = Event(), Event(), Event()
        attempts = []
        def unavailable(*_args, **_kwargs):
            attempts.append(1)
            entered.set()
            self.assertTrue(gate.wait(1))
            raise RegistryRequestError(503, "GET", "/blob", "busy")
        with patch.object(self.client, "blob_bytes", side_effect=unavailable):
            with ThreadPoolExecutor(1) as pool:
                future = pool.submit(cache.read, self.component, 0, 4, cancel=cancelled)
                self.assertTrue(entered.wait(1))
                cancelled.set()
                with self.assertRaises(CancelledError):
                    future.result(1)
                gate.set()
            cache.close()
        self.assertEqual(attempts, [1])
        self.assertEqual(cache.metrics()["fetch_retries"], 0)

    def test_retry_deadline_shrinks_transport_timeout_and_close_cancels_backoff(self):
        cache = self.cache(fetch_timeout_seconds=.12)
        timeouts = []
        def unavailable(*_args, timeout_seconds, **_kwargs):
            timeouts.append(timeout_seconds)
            raise RegistryRequestError(429, "GET", "/blob", "busy")
        started = time.monotonic()
        with patch.object(self.client, "blob_bytes", side_effect=unavailable):
            with self.assertRaisesRegex(TimeoutError, "fetch deadline"):
                cache.read(self.component, 0, 4)
        self.assertLess(time.monotonic() - started, .5)
        self.assertGreaterEqual(len(timeouts), 2)
        self.assertTrue(all(a > b > 0 for a, b in zip(timeouts, timeouts[1:])))
        entered = Event()
        def blocked(*_args, **_kwargs):
            entered.set()
            raise RegistryRequestError(503, "GET", "/blob", "busy")
        with patch.object(self.client, "blob_bytes", side_effect=blocked), ThreadPoolExecutor(1) as pool:
            future = pool.submit(cache.read, self.component, 0, 4)
            self.assertTrue(entered.wait(1))
            cache.close()
            with self.assertRaises(CancelledError):
                future.result(1)
