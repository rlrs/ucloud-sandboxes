"""ucloud-chunk-serve (runtime/chunk_serve, Go) in front of ucloud-chunk-store:
the same bytes, headers and statuses as the Python node for every read, and
everything it cannot answer from resident extents passed through."""
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import threading
import time
import unittest
from tempfile import TemporaryDirectory

from tests.chunk_store_support import REPOSITORY, ChunkStoreFixture, sample_images
from ucloud_sandboxes.chunk_index import http_request
from ucloud_sandboxes.chunk_store_node import (ChunkStoreClient, ChunkStoreNode, ChunkStoreServer, ExtentCache,
                                               S3Source, VirtualBlobs, locator_objects)
from ucloud_sandboxes.environment_artifact import load_environment
from ucloud_sandboxes.managed_registry import RegistryRequestError

TEST_TIER = "contract"
READ, WRITE = "r" * 32, "w" * 32
SOURCE = Path(__file__).resolve().parents[1] / "runtime/chunk_serve"


def build_binary(directory):
    go = shutil.which("go")
    if go is None:
        raise unittest.SkipTest("Go toolchain is unavailable")
    environment = {key: value for key, value in os.environ.items() if key not in ("GOARCH", "GOFLAGS", "GOOS")}
    environment.update(CGO_ENABLED="0", GOTOOLCHAIN="local", GOCACHE=str(directory / "go-cache"))
    binary = directory / "ucloud-chunk-serve"
    subprocess.run([go, "build", "-trimpath", "-o", str(binary), "."], cwd=SOURCE, env=environment, check=True,
                   capture_output=True, timeout=300)
    return binary


def request(method, url, token=READ, spec=None):
    """(status, headers that matter, body) of one request, errors included."""
    headers = {"Authorization": "Bearer " + token} if token else {}
    if spec:
        headers["Range"] = spec
    try:
        status, found, body = http_request(method, url, headers=headers, max_bytes=64 * 1024 ** 2)
    except RegistryRequestError as error:
        return error.status_code, None, None
    return status, {name: found.get(name) for name in ("Content-Length", "Content-Range")}, body


class ChunkServeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.built = TemporaryDirectory()
        cls.binary = build_binary(Path(cls.built.name))

    @classmethod
    def tearDownClass(cls):
        cls.built.cleanup()

    def setUp(self):
        kept = TemporaryDirectory()
        self.addCleanup(kept.cleanup)
        self.kept = Path(kept.name)
        os.environ["FAKE_NYDUS_KEEP"] = kept.name
        self.addCleanup(os.environ.pop, "FAKE_NYDUS_KEEP", None)
        self.store = ChunkStoreFixture(self)
        self.store.converter.nydusd_blobs = True
        sample_images(self.store.client)
        roots = [self.store.converter.convert(REPOSITORY, tag)["root"] for tag in ("a", "b")]
        self.cache = self.store.root / "node"
        node = ChunkStoreNode(ExtentCache(self.cache, 256 * 1024 ** 2),
                              S3Source(self.store.objects.presigner, "test/chunks", concurrency=4),
                              extent_bytes=1024 ** 2, warm_concurrency=2)
        self.addCleanup(node.close)
        self.python = ChunkStoreServer(("127.0.0.1", 0), node, read_token=READ, write_token=WRITE)
        self.python.blobs = VirtualBlobs(node, self.store.index.writer)
        threading.Thread(target=self.python.serve_forever, daemon=True).start()
        self.addCleanup(self.python.server_close)
        self.addCleanup(self.python.shutdown)
        self.python_url = f"http://127.0.0.1:{self.python.server_address[1]}"
        # Registration stores each blob's layout; then every object but one is warmed.
        self.store.index.service.store_url = self.python_url
        objects = {}
        self.components = []
        for root in roots:
            for component in load_environment(self.store.registry, root).components:
                loaded = self.store.registry.load(component)
                self.store.index.writer.register(component, loaded.bootstrap["digest"], loaded.chunk_map)
                self.components.append(component[7:])
                for item in locator_objects(self.store.index.reader.locator(component), self.python_url, READ):
                    objects[item["key"]] = item
        self.packs = sorted(key for key in objects if key.startswith("packs/"))
        self.cold = self.packs[-1]
        client = ChunkStoreClient(self.python_url, WRITE)
        job = client.warm([{"key": key, "ranges": None} for key in sorted(objects) if key != self.cold])
        self.assertEqual(client.wait(job["job"], timeout=60)["failed"], 0)
        self.native_url = self.start_native()

    def start_native(self):
        tokens = {}
        for name, token in (("read", READ), ("write", WRITE)):
            path = self.store.root / f"{name}.token"
            path.write_text(token + "\n")
            path.chmod(0o600)
            tokens[f"{name}_token_file"] = str(path)
        config = self.store.root / "chunk-store.json"
        config.write_text(json.dumps({**tokens, "nydusd": {"path": "/n", "sha256": "0" * 64}, "store_node": {
            "listen": "10.0.0.1:5091", "cache_dir": str(self.cache), "extent_bytes": 1024 ** 2}}))
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen([str(self.binary), "--config", str(config), "--listen", f"127.0.0.1:{port}",
                                    "--upstream", self.python_url.removeprefix("http://")],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.addCleanup(process.stderr.close)
        self.addCleanup(process.wait, 10)
        self.addCleanup(process.terminate)
        url = f"http://127.0.0.1:{port}"
        for _ in range(200):
            try:
                http_request("GET", url + "/healthz", max_bytes=1 << 20)
                return url
            except OSError:
                time.sleep(.05)
        self.fail("ucloud-chunk-serve never answered: " + process.stderr.read(2000).decode())

    def blobs(self):
        return [(component, path.name) for component in self.components[:1]
                for path in sorted(self.kept.iterdir()) if len(path.name) == 64]

    def native(self):
        return ChunkStoreClient(self.native_url, READ).metrics()["native"]

    def assertSame(self, path, method="GET", token=READ, spec=None):
        python, native = (request(method, base + path, token, spec) for base in (self.python_url, self.native_url))
        self.assertEqual(native, python, (path, method, spec))
        return native

    def test_every_read_matches_the_python_node(self):
        rng = random.Random(4)
        blobs = self.blobs()
        self.assertGreaterEqual(len(blobs), 3)
        for component, blob in blobs:
            path = f"/v2/virtual/{component}/blobs/sha256:{blob}"
            size = len((self.kept / blob).read_bytes())
            self.assertEqual(self.assertSame(path)[2], (self.kept / blob).read_bytes())
            self.assertSame(path, "HEAD")
            self.assertSame(path, "HEAD", spec="bytes=0-0")
            for _ in range(40):
                first = rng.randrange(size)
                spec = rng.choice([f"bytes={first}-{rng.randrange(first, size)}", f"bytes={first}-",
                                   f"bytes=-{rng.randrange(1, size + 5)}"])
                self.assertSame(path, spec=spec)
        for key in self.packs[:-1]:
            for _ in range(10):
                self.assertSame(f"/v1/objects/{key}", spec=f"bytes={rng.randrange(1 << 20)}-{rng.randrange(1 << 21)}")
            self.assertSame(f"/v1/objects/{key}", token=WRITE)
        served = self.native()
        self.assertGreater(served["served"], 100)
        self.assertEqual(served["proxied"]["miss"], 0)  # Unsatisfiable random ranges are the Python node's 416s.

    def test_errors_and_misses_are_the_python_nodes_answers(self):
        component, blob = self.blobs()[0]
        path = f"/v2/virtual/{component}/blobs/sha256:{blob}"
        size = len((self.kept / blob).read_bytes())
        for case in ({"token": "x" * 32}, {"token": ""}, {"spec": "bytes=1-2,4-5"}, {"spec": f"bytes={size}-"},
                     {"spec": "bytes=-0"}):
            self.assertSame(path, **case)
        unknown = "0" * 64
        for odd in (f"/v2/virtual/{component}/blobs/sha256:{unknown}", f"/v1/objects/packs/00/{unknown}.pack",
                    "/v1/objects/meta/secret.env", f"/v1/objects/packs/ab/{self.packs[0].split('/')[2]}"):
            self.assertSame(odd)
        # A cold pack: passed through and filled by the Python node, then served natively.
        before = self.native()["served"]
        self.assertEqual(request("GET", f"{self.native_url}/v1/objects/{self.cold}", spec="bytes=0-99")[0], 206)
        self.assertEqual(self.native()["served"], before)
        time.sleep(.3)  # One shard rescan later.
        self.assertSame(f"/v1/objects/{self.cold}", spec="bytes=0-99")
        self.assertEqual(self.native()["served"], before + 1)
        metrics = ChunkStoreClient(self.native_url, READ).metrics()
        self.assertIn("cache", metrics)  # The Python node's own metrics, with the native ones beside them.


if __name__ == "__main__":
    unittest.main()
