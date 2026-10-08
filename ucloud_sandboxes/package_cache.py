"""A package cache for builds: an HTTP proxy for allowlisted package mirrors.

Build steps (apt over plain HTTP) reach it through ``http_proxy``. Each request
names an allowlisted host; the cache fetches from that host's configured
upstream (``upstreams``: ``archive.ubuntu.com`` may be served by a faster
mirror of the same tree, as Canonical's archive stalls from UCloud) and keeps:

- package files (a path under ``/pool/`` or ``/by-hash/``) for good, since a
  path there never changes content, evicted least recently used past
  ``max_bytes``;
- index files (``dists/...``) for ``index_seconds``, then refetched; a stale
  copy is served when the upstream fails.

Concurrent requests for one file share one upstream fetch. Only GET and HEAD;
no CONNECT, so HTTPS goes direct. It runs on the store node, on the private
network (``chunk_store.store_node.package_cache``).
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import threading
import time
from urllib import error, parse, request

IMMUTABLE = re.compile(r"/(?:pool|by-hash)/")
MAX_OBJECT_BYTES = 4 * 1024 ** 3
_HOST = re.compile(r"[a-z0-9.-]+")
COPY_CHUNK = 1024 * 1024


@dataclass(frozen=True)
class PackageCacheConfig:
    """``chunk_store.store_node.package_cache``."""
    listen: str  # host:port on the private network
    url: str  # how build sandboxes name it (their http_proxy)
    cache_dir: str
    max_bytes: int
    # Allowlisted host -> the upstream origin serving the same paths.
    upstreams: dict = field(default_factory=dict)
    index_seconds: int = 600

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict) or set(raw) - {f for f in cls.__dataclass_fields__}:
            raise ValueError("chunk_store.store_node.package_cache fields do not match schema")
        result = cls(**raw)
        host, _, port = result.listen.rpartition(":")
        if not host or not port.isdigit() or not 0 < int(port) < 65536:
            raise ValueError("package_cache.listen must be host:port")
        if not result.url.startswith("http://") or not Path(result.cache_dir).is_absolute():
            raise ValueError("package_cache.url must be http:// and cache_dir absolute")
        if type(result.max_bytes) is not int or result.max_bytes < 1024 ** 3:
            raise ValueError("package_cache.max_bytes must be at least 1 GiB")
        if type(result.index_seconds) is not int or not 0 <= result.index_seconds <= 86400:
            raise ValueError("package_cache.index_seconds must be 0-86400")
        if not isinstance(result.upstreams, dict) or not result.upstreams:
            raise ValueError("package_cache.upstreams must allowlist at least one host")
        for host, origin in result.upstreams.items():
            parsed = parse.urlsplit(origin) if isinstance(origin, str) else None
            if (not _HOST.fullmatch(host or "") or parsed is None or parsed.scheme not in ("http", "https")
                    or not parsed.hostname or parsed.path not in ("", "/") or parsed.query):
                raise ValueError(f"package_cache.upstreams[{host!r}] must map a host to an http(s) origin")
        return result

    def to_dict(self):
        return asdict(self)


class PackageCache:
    def __init__(self, config, *, fetch=None, now=time.time):
        self.config, self.now = config, now
        self.root = Path(config.cache_dir)
        (self.root / "objects").mkdir(parents=True, exist_ok=True)
        (self.root / "tmp").mkdir(parents=True, exist_ok=True)
        self.fetch = fetch or self._fetch
        self._locks, self._guard = {}, threading.Lock()
        self.metrics = {"hits": 0, "misses": 0, "stale": 0, "errors": 0, "bytes_served": 0, "bytes_fetched": 0,
                        "evicted_bytes": 0}
        self._total = sum(p.stat().st_size for p in (self.root / "objects").glob("*/*"))

    def _count(self, name, value=1):
        with self._guard:
            self.metrics[name] += value

    def _lock(self, key):
        with self._guard:
            return self._locks.setdefault(key, threading.Lock())

    def used(self, path):
        """Mark a served object recently used: its atime, on the cache's clock, after the
        read (a read may move atime itself, depending on the filesystem's relatime)."""
        try:
            os.utime(path, (self.now(), path.stat().st_mtime))
        except FileNotFoundError:
            pass

    def upstream_url(self, url):
        """The upstream URL for an allowlisted absolute http URL, else None."""
        parsed = parse.urlsplit(url)
        host = (parsed.hostname or "").lower()
        if parsed.scheme != "http" or host not in self.config.upstreams or parsed.username or parsed.password:
            return None
        if ".." in parsed.path.split("/"):
            return None
        return self.config.upstreams[host].rstrip("/") + (parsed.path or "/")

    def path(self, url):
        digest = hashlib.sha256(url.encode()).hexdigest()
        return self.root / "objects" / digest[:2] / digest

    def get(self, url):
        """(status, path or None): a cached file for ``url`` (an allowlisted http URL)."""
        upstream = self.upstream_url(url)
        if upstream is None:
            return HTTPStatus.FORBIDDEN, None
        target, immutable = self.path(url), bool(IMMUTABLE.search(parse.urlsplit(url).path))
        fresh = lambda: target.exists() and (immutable or self.now() - target.stat().st_mtime  # noqa: E731
                                              < self.config.index_seconds)
        if fresh():
            self._count("hits")
            return HTTPStatus.OK, target
        with self._lock(url):
            if fresh():
                self._count("hits")
                return HTTPStatus.OK, target
            self._count("misses")
            try:
                status = self.fetch(upstream, target)
            except (OSError, error.URLError):
                self._count("errors")
                if target.exists():  # A stale index beats none.
                    self._count("stale")
                    return HTTPStatus.OK, target
                return HTTPStatus.BAD_GATEWAY, None
            if status != HTTPStatus.OK:
                if int(status) >= 500 and target.exists():  # The upstream failed: a stale index beats none.
                    self._count("stale")
                    return HTTPStatus.OK, target
                return status, None
        if self._total > self.config.max_bytes:
            self._sweep()
        return HTTPStatus.OK, target

    def _fetch(self, upstream, target):
        req = request.Request(upstream, headers={"User-Agent": "ucloud-package-cache"})
        try:
            response = request.urlopen(req, timeout=60)
        except error.HTTPError as exc:
            return exc.code if exc.code in (403, 404, 410) else HTTPStatus.BAD_GATEWAY
        with response, tempfile.NamedTemporaryFile(dir=self.root / "tmp", delete=False) as out:
            size = 0
            while chunk := response.read(COPY_CHUNK):
                size += len(chunk)
                if size > MAX_OBJECT_BYTES:
                    raise OSError("object exceeds the package cache's object limit")
                out.write(chunk)
        target.parent.mkdir(parents=True, exist_ok=True)
        replaced = target.stat().st_size if target.exists() else 0
        os.replace(out.name, target)
        os.utime(target, (self.now(), self.now()))  # Freshness and recency on the cache's own clock.
        self._count("bytes_fetched", size)
        with self._guard:
            self._total += size - replaced
        return HTTPStatus.OK

    def _sweep(self):
        """Least recently used objects go, down to 90% of max_bytes."""
        files = [(p.stat().st_atime, p.stat().st_size, p) for p in (self.root / "objects").glob("*/*")]
        total = sum(size for _, size, _ in files)
        for _, size, path in sorted(files):
            if total <= self.config.max_bytes * 0.9:
                break
            try:
                path.unlink()
            except FileNotFoundError:
                continue
            total -= size
            self._count("evicted_bytes", size)
        with self._guard:
            self._total = total


class _Handler(BaseHTTPRequestHandler):
    cache: PackageCache = None
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _url(self):
        if self.path.startswith("http://"):
            return self.path
        return f"http://{(self.headers.get('Host') or '').split(':')[0]}{self.path}"

    def _send(self, status, body=b"", content_type="text/plain"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _serve(self):
        if self.path == "/healthz":
            return self._send(HTTPStatus.OK, json.dumps({"ok": True, **self.cache.metrics}).encode(),
                              "application/json")
        status, path = self.cache.get(self._url())
        if path is None:
            return self._send(int(status), f"{int(status)}\n".encode())
        size = path.stat().st_size
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Length", str(size))
        self.send_header("Content-Type", "application/octet-stream")
        self.end_headers()
        if self.command == "HEAD":
            return
        with path.open("rb") as source:
            shutil.copyfileobj(source, self.wfile, COPY_CHUNK)
        self.cache.used(path)
        self.cache._count("bytes_served", size)

    def do_GET(self):  # noqa: N802
        self._serve()

    def do_HEAD(self):  # noqa: N802
        self._serve()

    def do_CONNECT(self):  # noqa: N802
        self._send(HTTPStatus.METHOD_NOT_ALLOWED, b"no CONNECT: HTTPS goes direct\n")

    do_POST = do_PUT = do_DELETE = do_CONNECT  # noqa: N815


def serve(config):
    handler = type("PackageCacheHandler", (_Handler,), {"cache": PackageCache(config)})
    host, port = config.listen.rsplit(":", 1)
    server = ThreadingHTTPServer((host, int(port)), handler)
    server.daemon_threads = True
    server.serve_forever()
