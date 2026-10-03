from concurrent.futures import CancelledError, ThreadPoolExecutor
from dataclasses import replace
import json
import os
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

TEST_TIER = "contract"


class MemoryRegistry:
    def __init__(self):
        self.blobs, self.manifests, self.uploads = {}, {}, {}
        self.tags = {}
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
        self.tags[tag] = content_digest(payload)

    def manifest_document(self, repository, digest):
        digest = self.tags.get(digest, digest) if digest not in self.manifests else digest
        if digest not in self.manifests:
            raise RegistryRequestError(404, "GET", digest, "MANIFEST_UNKNOWN")
        return json.loads(self.manifests[digest]), {}

    def blob_bytes(self, repository, digest, *, max_bytes, timeout_seconds=None):
        self.reads.append(digest)
        self.entered.set()
        if self.gate is not None:
            self.gate.wait(3)
        return self.blobs[digest][:max_bytes + 1]

    def upload_blob_file(self, repository, path, digest, size):
        data = Path(path).read_bytes()
        assert len(data) == size
        if content_digest(data) != digest:
            raise RegistryRequestError(400, "PUT", "/v2/blobs/uploads", "DIGEST_INVALID")
        self.blobs[digest] = data
        return digest

    def blob_range(self, repository, digest, offset, length, *, timeout_seconds=None):
        data = self.blobs[digest][offset:offset + length]
        # A range read of one chunk returns exactly that chunk's bytes.
        self.reads.append(content_digest(data))
        self.entered.set()
        if self.gate is not None:
            self.gate.wait(3)
        return data


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
        # With the signed blob stored, publication would reference it and never
        # upload the changed file; without it the registry rejects the upload.
        self.client.blobs.pop(self.component.image_digest, None)
        with self.assertRaisesRegex(RegistryRequestError, "DIGEST_INVALID"):
            self.registry.publish(self.image, self.component, tag="changed")
        self.assertEqual(len(self.client.manifests), 1)

    def test_only_demanded_chunks_download_and_corruption_never_reaches_reader(self):
        cache = self.cache(max_bytes=CHUNK_BYTES, demand_window_chunks=1)  # One chunk per miss.
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
        # Corrupt the third chunk's bytes inside the published image blob.
        blob = bytearray(self.client.blobs[self.component.image_digest])
        blob[2 * CHUNK_BYTES:2 * CHUNK_BYTES + 4096] = b"x" * 4096
        self.client.blobs[self.component.image_digest] = bytes(blob)
        with self.assertRaisesRegex(ValueError, "content identity"):
            cache.read(self.component, 2 * CHUNK_BYTES, 4)
        with self.assertRaises(ValueError):
            cache.read(self.component, len(self.bytes) - 1, 2)

    def test_shared_miss_survives_one_reader_cancellation(self):
        cache = self.cache(concurrent_misses=1, demand_window_chunks=1)
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


class EnvironmentDocumentLoadTests(unittest.TestCase):
    setUp = EnvironmentArtifactTests.setUp

    def test_supplied_document_skips_only_manifest_read_and_load_remains_compatible(self):
        document, _ = self.client.manifest_document("environments", self.digest)
        reader = EnvironmentArtifactRegistry(self.client, "environments", self.registry.trusted_keys)
        self.client.reads.clear()
        with patch.object(self.client, "manifest_document", wraps=self.client.manifest_document) as manifests:
            self.assertEqual(reader.load_document(self.digest, document), self.component)
            manifests.assert_not_called()
            self.assertEqual(self.client.reads, [document["config"]["digest"]])
            self.assertTrue(reader.whole_image(self.component))
            self.assertEqual(reader.load(self.digest), self.component)
            manifests.assert_called_once_with("environments", self.digest)

    def test_supplied_document_rejects_invalid_metadata_and_wrong_manifest_binding(self):
        from ucloud_sandboxes.environment_artifact import canonical_bytes
        document, _ = self.client.manifest_document("environments", self.digest)
        reader = EnvironmentArtifactRegistry(self.client, "environments", self.registry.trusted_keys)
        for malformed in (None, [], "manifest"):
            with self.subTest(malformed=malformed), self.assertRaisesRegex(ValueError, "OCI metadata"):
                reader.load_document(self.digest, malformed)
        changed = document | {"annotations": {"unexpected": "manifest"}}
        with self.assertRaisesRegex(ValueError, "manifest content identity"):
            reader.load_document(self.digest, changed)
        for changes in ({"schemaVersion": 1}, {"config": {}}, {"config": []}):
            changed = document | changes
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "OCI metadata"):
                reader.load_document(content_digest(canonical_bytes(changed)), changed)
        self.assertFalse(reader.whole_image(self.component))

    def test_supplied_document_authenticates_config_and_dependency_closure(self):
        from ucloud_sandboxes.environment_artifact import canonical_bytes
        document, _ = self.client.manifest_document("environments", self.digest)
        reader = EnvironmentArtifactRegistry(self.client, "environments", self.registry.trusted_keys)
        changed = document | {"layers": []}
        with self.assertRaisesRegex(ValueError, "dependency closure"):
            reader.load_document(content_digest(canonical_bytes(changed)), changed)
        self.assertFalse(reader.whole_image(self.component))
        untrusted = EnvironmentArtifactRegistry(self.client, "environments", {})
        with self.assertRaisesRegex(ValueError, "not trusted"):
            untrusted.load_document(self.digest, document)
        config_digest = document["config"]["digest"]
        original = self.client.blobs[config_digest]
        self.client.blobs[config_digest] = b"x" * len(original)
        with self.assertRaisesRegex(ValueError, "index content identity"):
            reader.load_document(self.digest, document)
        changed_config = json.loads(original) | {"source_image": "sha256:" + "2" * 64}
        raw = canonical_bytes(changed_config)
        self.client.blobs[content_digest(raw)] = raw
        changed = document | {"config": document["config"] | {"digest": content_digest(raw), "size": len(raw)}}
        with self.assertRaisesRegex(ValueError, "signature"):
            reader.load_document(content_digest(canonical_bytes(changed)), changed)


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
            cache._fetch(self.component.chunks[0], cancelled, (self.component.image_digest, 0))
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
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
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
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
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
            with self.subTest(failure=failure), patch.object(self.client, "blob_range", side_effect=failure) as read:
                with self.assertRaises(RegistryRequestError):
                    cache.read(self.component, 0, 4)
                self.assertEqual(read.call_count, 1)
        with patch.object(self.client, "blob_range", return_value=b"x" * CHUNK_BYTES) as read:
            with self.assertRaisesRegex(ValueError, "content identity"):
                cache.read(self.component, 0, 4)
            self.assertEqual(read.call_count, 1)
        self.assertEqual(cache.metrics()["fetch_retries"], 0)
        self.assertEqual(cache.metrics()["cached_bytes"], 0)
        with patch.object(self.client, "blob_range",
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
        with patch.object(self.client, "blob_range", side_effect=unavailable):
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
        with patch.object(self.client, "blob_range", side_effect=unavailable):
            with self.assertRaisesRegex(TimeoutError, "fetch deadline"):
                cache.read(self.component, 0, 4)
        self.assertLess(time.monotonic() - started, .5)
        self.assertGreaterEqual(len(timeouts), 2)
        self.assertTrue(all(a > b > 0 for a, b in zip(timeouts, timeouts[1:])))
        entered = Event()
        def blocked(*_args, **_kwargs):
            entered.set()
            raise RegistryRequestError(503, "GET", "/blob", "busy")
        with patch.object(self.client, "blob_range", side_effect=blocked), ThreadPoolExecutor(1) as pool:
            future = pool.submit(cache.read, self.component, 0, 4)
            self.assertTrue(entered.wait(1))
            cache.close()
            with self.assertRaises(CancelledError):
                future.result(1)


class UploadBlobRetryTests(unittest.TestCase):
    def client(self, failures):
        from unittest.mock import Mock
        from ucloud_sandboxes.managed_registry import RegistryRequestError
        client = Mock()
        client.blob_exists.return_value = False
        client.start_blob_upload.return_value = "/v2/environments/blobs/uploads/x"
        client.upload_blob_chunk.return_value = "/v2/environments/blobs/uploads/x"
        client.finish_blob_upload.side_effect = [
            RegistryRequestError(code, "PUT", "/x", "err") if code else "sha256:ok"
            for code in failures
        ]
        client.abort_blob_upload.side_effect = RegistryRequestError(500, "DELETE", "/x", "append to zero-size path")
        return client

    def test_transient_commit_failure_is_retried_and_abort_errors_do_not_mask(self):
        from unittest.mock import patch
        from ucloud_sandboxes import environment_artifact
        payload = b"chunk"
        client = self.client([503, None])
        with patch.object(environment_artifact.time, "sleep"), \
                self.assertLogs("ucloud_sandboxes.environment_artifact", level="WARNING"):
            environment_artifact._upload_blob(client, "environments", payload,
                                              environment_artifact.content_digest(payload))
        self.assertEqual(client.finish_blob_upload.call_count, 2)

    def test_permanent_failure_surfaces_its_own_error(self):
        from ucloud_sandboxes import environment_artifact
        from ucloud_sandboxes.managed_registry import RegistryRequestError
        payload = b"chunk"
        client = self.client([400])
        with self.assertRaises(RegistryRequestError) as caught, \
                self.assertLogs("ucloud_sandboxes.environment_artifact", level="WARNING"):
            environment_artifact._upload_blob(client, "environments", payload,
                                              environment_artifact.content_digest(payload))
        self.assertEqual(caught.exception.status_code, 400)
        self.assertEqual(client.finish_blob_upload.call_count, 1)


class EnvironmentLayoutTests(EnvironmentArtifactTests.__mro__[0]):
    """Whole-image and per-chunk publications both load and read."""

    def test_earlier_per_chunk_publication_still_loads_and_reads(self):
        from ucloud_sandboxes import environment_artifact as artifact
        for chunk, offset in zip(self.component.chunks, range(0, len(self.bytes), CHUNK_BYTES)):
            artifact._upload_blob(self.client, "environments", self.bytes[offset:offset + chunk.size], chunk.digest)
        config = artifact.canonical_bytes(self.component.to_dict())
        manifest = artifact.canonical_bytes({"schemaVersion": 2, "mediaType": artifact.OCI_IMAGE,
            "config": {"mediaType": artifact.COMPONENT_MEDIA_TYPE, "digest": artifact.content_digest(config), "size": len(config)},
            "layers": [{"mediaType": artifact.CHUNK_MEDIA_TYPE, **c.to_dict()} for c in self.component.chunks]})
        self.client.put_manifest("environments", "per-chunk", manifest, media_type=artifact.OCI_IMAGE)
        reader = EnvironmentArtifactRegistry(self.client, "environments", self.registry.trusted_keys)
        self.assertEqual(reader.load(artifact.content_digest(manifest)), self.component)
        self.assertFalse(reader.whole_image(self.component))
        self.client.reads.clear()
        cache = VerifiedEnvironmentCache(self.root / "per-chunk-cache", reader)
        self.addCleanup(cache.close)
        self.assertEqual(cache.read(self.component, CHUNK_BYTES - 5, 10), b"a" * 5 + b"b" * 5)
        self.assertEqual(self.client.reads, [c.digest for c in self.component.chunks[:2]])

    def test_whole_image_publication_is_one_blob_read_by_range(self):
        self.assertTrue(self.registry.whole_image(self.component))
        self.assertIn(self.component.image_digest, self.client.blobs)
        self.assertFalse(any(c.digest in self.client.blobs for c in self.component.chunks
                             if c.digest != self.component.image_digest))


class RegistryRangeTests(unittest.TestCase):
    def test_blob_range_requires_partial_content(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from threading import Thread
        from ucloud_sandboxes.managed_registry import RegistryClient
        payload = bytes(range(256)) * 16
        honour = [True]

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(inner):
                start, end = map(int, inner.headers["Range"].removeprefix("bytes=").split("-"))
                if honour[0]:
                    body = payload[start:end + 1]
                    inner.send_response(206)
                    inner.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
                else:
                    body = payload
                    inner.send_response(200)
                inner.send_header("Content-Length", str(len(body)))
                inner.end_headers()
                inner.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        client = RegistryClient(f"http://127.0.0.1:{server.server_address[1]}")
        digest = "sha256:" + "a" * 64
        self.assertEqual(client.blob_range("environments", digest, 100, 50), payload[100:150])
        honour[0] = False
        with self.assertRaisesRegex(ValueError, "requested blob range"):
            client.blob_range("environments", digest, 100, 50)

    def test_upload_blob_file_streams_the_whole_file_in_one_put(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from threading import Thread
        from ucloud_sandboxes.managed_registry import RegistryClient
        payload = os.urandom(3 * 1024 * 1024 + 17)
        digest = content_digest(payload)
        received = {}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(inner):
                inner.send_response(202)
                inner.send_header("Location", "/v2/environments/blobs/uploads/u1?_state=s")
                inner.send_header("Content-Length", "0")
                inner.end_headers()

            def do_PUT(inner):
                received["path"] = inner.path
                received["body"] = inner.rfile.read(int(inner.headers["Content-Length"]))
                inner.send_response(201)
                inner.send_header("Docker-Content-Digest", digest)
                inner.send_header("Content-Length", "0")
                inner.end_headers()

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        client = RegistryClient(f"http://127.0.0.1:{server.server_address[1]}")
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "image"
            path.write_bytes(payload)
            self.assertEqual(client.upload_blob_file("environments", path, digest, len(payload)), digest)
        self.assertEqual(received["body"], payload)
        self.assertIn("digest=sha256%3A", received["path"])
