"""The build package cache: an allowlisted, remapping, caching HTTP proxy."""
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import time
import unittest
from urllib import error, request

from ucloud_sandboxes.package_cache import PackageCache, PackageCacheConfig, _Handler


class Mirror:
    """An upstream mirror: counts requests per path, can slow down or fail."""

    def __init__(self):
        self.hits, self.content, self.delay, self.down = {}, {}, 0.0, False
        mirror = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):  # noqa: N802
                mirror.hits[self.path] = mirror.hits.get(self.path, 0) + 1
                time.sleep(mirror.delay)
                body = mirror.content.get(self.path)
                if mirror.down or body is None:
                    self.send_response(503 if mirror.down else 404)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.origin = f"http://127.0.0.1:{self.server.server_address[1]}"


class PackageCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.mirror = Mirror()
        self.addCleanup(self.mirror.server.shutdown)
        self.clock = [1000.0]
        config = PackageCacheConfig.from_dict({
            "listen": "127.0.0.1:3142", "url": "http://127.0.0.1:3142", "cache_dir": str(Path(self.temp.name) / "c"),
            "max_bytes": 1024 ** 3, "index_seconds": 600,
            # Canonical's archive, served by a faster mirror of the same tree.
            "upstreams": {"archive.ubuntu.com": self.mirror.origin}})
        self.cache = PackageCache(config, now=lambda: self.clock[0])
        handler = type("H", (_Handler,), {"cache": self.cache})
        self.proxy = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=self.proxy.serve_forever, daemon=True).start()
        self.addCleanup(self.proxy.shutdown)
        port = self.proxy.server_address[1]
        self.opener = request.build_opener(request.ProxyHandler({"http": f"http://127.0.0.1:{port}"}))

    def get(self, url):
        try:
            with self.opener.open(url, timeout=10) as response:
                return response.status, response.read()
        except error.HTTPError as exc:
            return exc.code, b""

    def test_packages_are_fetched_once_from_the_remapped_upstream(self):
        path = "/ubuntu/pool/main/g/gawk/gawk_5.1.0_amd64.deb"
        self.mirror.content[path] = b"deb bytes"
        self.mirror.delay = 0.2
        with ThreadPoolExecutor(8) as pool:  # Concurrent requests share one fetch.
            results = list(pool.map(lambda _: self.get("http://archive.ubuntu.com" + path), range(8)))
        self.assertEqual(results, [(200, b"deb bytes")] * 8)
        self.assertEqual(self.mirror.hits[path], 1)
        self.clock[0] += 10 ** 6  # Package paths never change: no refetch, ever.
        self.assertEqual(self.get("http://archive.ubuntu.com" + path), (200, b"deb bytes"))
        self.assertEqual(self.mirror.hits[path], 1)
        self.assertEqual(self.cache.metrics["hits"], 8)

    def test_indexes_expire_and_a_stale_one_beats_a_failing_upstream(self):
        path = "/ubuntu/dists/jammy/InRelease"
        self.mirror.content[path] = b"v1"
        self.assertEqual(self.get("http://archive.ubuntu.com" + path), (200, b"v1"))
        self.mirror.content[path] = b"v2"
        self.assertEqual(self.get("http://archive.ubuntu.com" + path), (200, b"v1"))  # Within index_seconds.
        self.clock[0] += 601
        self.assertEqual(self.get("http://archive.ubuntu.com" + path), (200, b"v2"))
        self.clock[0] += 601
        self.mirror.down = True
        self.assertEqual(self.get("http://archive.ubuntu.com" + path), (200, b"v2"))
        self.assertEqual(self.cache.metrics["stale"], 1)

    def test_only_allowlisted_plain_http_gets(self):
        self.assertEqual(self.get("http://example.org/ubuntu/pool/x.deb")[0], 403)
        self.assertEqual(self.get("http://archive.ubuntu.com/ubuntu/pool/missing.deb")[0], 404)
        self.assertEqual(self.get("http://archive.ubuntu.com/ubuntu/../etc/passwd")[0], 403)
        req = request.Request("http://archive.ubuntu.com/x", data=b"x", method="POST")
        with self.assertRaises(error.HTTPError) as caught:
            self.opener.open(req, timeout=10)
        self.assertEqual(caught.exception.code, 405)

    def test_least_recently_used_packages_are_evicted_past_the_limit(self):
        self.cache.config = PackageCacheConfig(**{**self.cache.config.to_dict(), "max_bytes": 1024 ** 3})
        for name in ("a", "b", "c"):
            self.mirror.content[f"/ubuntu/pool/{name}.deb"] = b"x" * 1000
            self.get(f"http://archive.ubuntu.com/ubuntu/pool/{name}.deb")
            self.clock[0] += 1
        self.get("http://archive.ubuntu.com/ubuntu/pool/a.deb")  # a is used again: b is now least recent.
        object.__setattr__(self.cache.config, "max_bytes", 2500)
        self.cache._sweep()
        kept = {name for name in "abc" if self.cache.path(f"http://archive.ubuntu.com/ubuntu/pool/{name}.deb").exists()}
        self.assertEqual(kept, {"a", "c"})  # Down to 90% of 2500: b, the least recently used, goes.

    def test_config_refuses_open_proxies_and_bad_origins(self):
        base = {"listen": "0.0.0.0:3142", "url": "http://10.0.0.1:3142", "cache_dir": "/var/lib/c",
                "max_bytes": 2 * 1024 ** 3}
        for upstreams in ({}, {"*": "http://x"}, {"archive.ubuntu.com": "ftp://x"},
                          {"archive.ubuntu.com": "http://x/ubuntu"}):
            with self.subTest(upstreams=upstreams), self.assertRaises(ValueError):
                PackageCacheConfig.from_dict({**base, "upstreams": upstreams})


if __name__ == "__main__":
    unittest.main()
