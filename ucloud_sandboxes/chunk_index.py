"""``ucloud-chunk-index``: the chunk store's SQLite index and its HTTP API.

Only builders and GC use the index (docs/chunk-store-design.md §1.3, §5).
Workers ask it for one thing, a root's unsigned locator: with presigned GET
URLs (decision 2), or with a store node configured (C2.6) the node's URLs,
so the S3 key stays on this service, the builders and the store node.
Everything here is a derived cache of pack footers and chunk maps in S3.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import re
import sqlite3
import threading
import time
from urllib.parse import parse_qs, quote, urlsplit

from .chunk_store import (MAX_BOOTSTRAP_BYTES, MAX_MAP_ENTRIES, MAX_PACK_BYTES, ChunkMap, Locator, PACK_TRAILER,
                          blob_layout_key, bootstrap_key, chunk_map_key, decode_tail_table, locator_key, pack_key,
                          parse_bootstrap, parse_pack_tail, require_hex, tail_key, zstd_content_size,
                          zstd_decompress)
from .environment_artifact import RAFS_MAX_CHUNK_MAP_BYTES, content_digest, require_digest
from .managed_registry import RegistryRequestError

_LOG = logging.getLogger(__name__)
LAYER_CLAIM_SECONDS = 30 * 60
# A builder holds a chunk reservation for one pack's fill and upload; a dead
# builder's reservations lapse after this and the next asker packs the chunk.
CHUNK_RESERVE_SECONDS = 10 * 60
RESERVED, KNOWN, BUSY = 0, 1, 2  # reserve() states, one byte per id on the wire.
MAX_TAIL_BYTES = 256 * 1024 ** 2  # A blob's tail object (chunk_store.encode_tail).
_MAX_JSON = 1024 ** 2
_SCHEMA = """
CREATE TABLE IF NOT EXISTS chunks (id BLOB PRIMARY KEY, pack INTEGER, off INTEGER, clen INTEGER,
    ulen INTEGER, flags INTEGER, condemned INTEGER DEFAULT 0) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS packs (pack INTEGER PRIMARY KEY, digest BLOB UNIQUE, bytes INTEGER, chunks INTEGER,
    origin_root BLOB, created INTEGER, state TEXT);
CREATE TABLE IF NOT EXISTS layers (diff_id BLOB, converter TEXT, bootstrap BLOB, created INTEGER,
    claimed_until INTEGER, owner TEXT, PRIMARY KEY (diff_id, converter)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS roots (component BLOB PRIMARY KEY, chunk_map BLOB, bootstrap BLOB,
    registered INTEGER, epoch INTEGER) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS root_packs (component BLOB, pack INTEGER, bytes INTEGER,
    PRIMARY KEY (component, pack)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS reservations (id BLOB PRIMARY KEY, owner TEXT, until INTEGER) WITHOUT ROWID;
"""


# --- SigV4 query presigning and a plain HTTP range reader ---

@dataclass(frozen=True)
class S3Presigner:
    """AWS Signature V4 presigned URLs (UNSIGNED-PAYLOAD, host header only)."""
    endpoint: str
    bucket: str
    region: str
    access_key_id: str
    secret_access_key: str
    security_token: str = ""
    path_style: bool = False

    def url(self, key, *, method="GET", expires=86400, now=None):
        if not 1 <= expires <= 7 * 86400:
            raise ValueError("presigned URL lifetime must be within SigV4's seven days")
        origin = urlsplit(self.endpoint)
        host = origin.netloc if self.path_style else f"{self.bucket}.{origin.netloc}"
        path = ("/" + quote(self.bucket, safe="") if self.path_style else "") + "/" + quote(key, safe="/-_.~")
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(time.time() if now is None else now))
        scope = f"{stamp[:8]}/{self.region}/s3/aws4_request"
        query = {"X-Amz-Algorithm": "AWS4-HMAC-SHA256", "X-Amz-Credential": f"{self.access_key_id}/{scope}",
                 "X-Amz-Date": stamp, "X-Amz-Expires": str(expires), "X-Amz-SignedHeaders": "host"}
        if self.security_token:
            query["X-Amz-Security-Token"] = self.security_token
        canonical_query = "&".join(f"{quote(name, safe='-_.~')}={quote(value, safe='-_.~')}"
                                   for name, value in sorted(query.items()))
        canonical = "\n".join((method, path, canonical_query, f"host:{host}\n", "host", "UNSIGNED-PAYLOAD"))
        signed = "\n".join(("AWS4-HMAC-SHA256", stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()))
        key_bytes = ("AWS4" + self.secret_access_key).encode()
        for part in (stamp[:8], self.region, "s3", "aws4_request"):
            key_bytes = hmac.new(key_bytes, part.encode(), hashlib.sha256).digest()
        signature = hmac.new(key_bytes, signed.encode(), hashlib.sha256).hexdigest()
        return f"{origin.scheme}://{host}{path}?{canonical_query}&X-Amz-Signature={signature}"


def redact(url):
    """A presigned URL is a bearer credential: never log its query."""
    return url.split("?", 1)[0]


_POOL = None
_POOL_LOCK = threading.Lock()


def _pool():
    global _POOL
    with _POOL_LOCK:
        if _POOL is None:
            import urllib3
            # Keep-alive matters at S3 latency: one TLS handshake per window
            # would double a demand miss.
            _POOL = urllib3.PoolManager(num_pools=64, maxsize=32, block=False, retries=False)
        return _POOL


def http_request(method, url, *, headers=None, body=None, timeout=30.0, max_bytes):
    """(status, headers, body) of one bounded request; HTTP errors raise.

    Errors are RegistryRequestError (status) or OSError (transport), the
    classes VerifiedEnvironmentCache already retries or refuses.
    """
    import urllib3
    deadline = time.monotonic() + timeout
    try:
        response = _pool().request(method, url, headers=headers or {}, body=body, preload_content=False,
                                   timeout=urllib3.Timeout(connect=min(timeout, 10.0), read=timeout))
        chunks, size, complete = [], 0, False
        try:
            while size <= max_bytes:
                if time.monotonic() >= deadline:
                    raise TimeoutError("chunk store request deadline exceeded")
                chunk = response.read(min(1 << 20, max_bytes + 1 - size))
                if not chunk:
                    complete = True
                    break
                chunks.append(chunk)
                size += len(chunk)
            payload = b"".join(chunks)
        finally:
            # Only a fully read response may return its connection to the pool.
            response.release_conn() if complete else response.close()
    except urllib3.exceptions.HTTPError as exc:
        raise OSError(f"chunk store request failed: {type(exc).__name__}") from exc
    if response.status >= 400:
        raise RegistryRequestError(response.status, method, redact(url), payload[:256].decode(errors="replace"))
    if len(payload) > max_bytes:
        raise ValueError("chunk store response exceeds its bound")
    return response.status, response.headers, payload


def http_range(url, start, length, *, timeout=30.0, headers=None):
    """Exactly ``length`` bytes at ``start``, or the last ``length`` with ``start=None``."""
    if length <= 0 or (start is not None and start < 0):
        raise ValueError("invalid chunk store range")
    spec = f"bytes=-{length}" if start is None else f"bytes={start}-{start + length - 1}"
    status, headers, payload = http_request("GET", url, headers={**(headers or {}), "Range": spec}, timeout=timeout,
                                            max_bytes=length)
    content_range = str(headers.get("Content-Range") or "")
    if start is None:
        if status not in (200, 206):  # 200 or "bytes 0-": the whole, smaller object.
            raise ValueError("object store did not serve the requested range")
        return payload
    if status != 206 or not content_range.startswith(f"bytes {start}-{start + length - 1}/") or len(payload) != length:
        raise ValueError("object store did not serve the requested range")
    return payload


# --- Object store: S3 writes with credentials, reads through presigned URLs ---

class ChunkObjectStore:
    """Packs and metadata under ``prefix``; ``client`` is a Boto3S3ObjectClient."""

    def __init__(self, client, presigner, prefix, *, url_seconds=86400, reader=http_range, getter=None):
        self.client, self.presigner, self.prefix = client, presigner, prefix.strip("/")
        self.url_seconds, self.reader = url_seconds, reader
        self.getter = getter or (lambda url, limit: http_request("GET", url, max_bytes=limit)[2])

    def url(self, key):
        return self.presigner.url(key, expires=self.url_seconds)

    def size(self, key):
        found = self.client.stat(key)
        return None if found is None else found.size

    def put_file(self, key, path, size):
        """Idempotent: content-addressed keys are never overwritten."""
        if self.size(key) == size:
            return False
        self.client.put_file(key, path, sha256=key.rsplit("/", 1)[-1].split(".", 1)[0])
        return True

    def put_bytes(self, key, payload):
        if self.size(key) == len(payload):
            return False
        self.client.put_bytes(key, payload, sha256=key.rsplit("/", 1)[-1].split(".", 1)[0])
        return True

    def get(self, key, max_bytes):
        return self.getter(self.url(key), max_bytes)

    def pack_entries(self, digest, size):
        """Footer of a durable pack: one suffix GET, two for large footers."""
        key = pack_key(self.prefix, digest)
        if self.size(key) != size:
            raise ValueError("pack is not durable in the object store")
        url = self.url(key)
        tail = self.reader(url, None, min(size, 256 * 1024))
        entries = parse_pack_tail(tail, size)
        if entries is None:
            footer_len, = PACK_TRAILER.unpack_from(tail, len(tail) - PACK_TRAILER.size)[:1]
            entries = parse_pack_tail(self.reader(url, None, footer_len + PACK_TRAILER.size), size)
        return entries


# --- The SQLite index ---

class UnknownRoot(LookupError):
    pass


class MissingChunks(ValueError):
    pass


class ChunkIndex:
    """One writer, concurrent WAL readers; ids are 32-byte blobs."""

    def __init__(self, path):
        self.path = str(path)
        self._write_lock = threading.Lock()
        self._local = threading.local()
        self._writer = self._connect()
        self._writer.executescript(_SCHEMA)

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
        connection.execute("PRAGMA journal_mode=WAL")
        # A commit reply promises the rows exist; keep them across power loss.
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _reader(self):
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = self._local.connection = self._connect()
        return connection

    def _rows(self, query, ids, connection=None):
        connection = connection or self._reader()
        found = {}
        for start in range(0, len(ids), 900):
            batch = ids[start:start + 900]
            found.update((row[0], row[1:]) for row in connection.execute(
                query.format(",".join("?" * len(batch))), batch))
        return found

    def locate(self, ids):
        """[(pack digest hex, offset, clen, flags) or None] for each id."""
        rows = self._rows("SELECT c.id, p.digest, c.off, c.clen, c.flags, c.condemned FROM chunks c "
                          "JOIN packs p ON p.pack = c.pack WHERE c.id IN ({})", list(dict.fromkeys(ids)))
        return [None if chunk_id not in rows or rows[chunk_id][4] else
                (rows[chunk_id][0].hex(), *rows[chunk_id][1:4]) for chunk_id in ids]

    def reserve(self, ids, owner, now=None, seconds=CHUNK_RESERVE_SECONDS):
        """KNOWN (live, design §3 step 3), RESERVED (``owner`` packs it) or BUSY, per id.

        Builders reserve before packing, so converters running at once never
        upload one chunk twice (M1 gate run 2: 2.3 GB of dead pack bytes with
        per-pack commits alone). Reserving again renews ``owner``'s hold.
        """
        now = int(time.time() if now is None else now)
        unique = list(dict.fromkeys(ids))
        states = {}
        with self._write_lock:
            self._writer.execute("BEGIN IMMEDIATE")
            try:
                known = self._rows("SELECT id, condemned FROM chunks WHERE id IN ({})", unique, self._writer)
                held = self._rows("SELECT id, owner, until FROM reservations WHERE id IN ({})", unique,
                                  self._writer)
                for chunk_id in unique:
                    if chunk_id in known and not known[chunk_id][0]:
                        states[chunk_id] = KNOWN
                    elif chunk_id in held and held[chunk_id][0] != owner and held[chunk_id][1] > now:
                        states[chunk_id] = BUSY
                    else:
                        states[chunk_id] = RESERVED
                self._writer.executemany(
                    "INSERT INTO reservations (id, owner, until) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE "
                    "SET owner = excluded.owner, until = excluded.until",
                    [(chunk_id, owner, now + seconds) for chunk_id, state in states.items() if state == RESERVED])
                self._writer.execute("COMMIT")
            except BaseException:
                self._writer.execute("ROLLBACK")
                raise
        return [states[chunk_id] for chunk_id in ids]

    def claim_layer(self, diff_id, converter, owner, now=None, seconds=LAYER_CLAIM_SECONDS):
        """complete (with its bootstrap), claimed (by ``owner``) or busy."""
        now = int(time.time() if now is None else now)
        key = bytes.fromhex(require_digest(diff_id)[7:])
        with self._write_lock:
            self._writer.execute("BEGIN IMMEDIATE")
            try:
                row = self._writer.execute("SELECT bootstrap, claimed_until, owner FROM layers "
                                           "WHERE diff_id = ? AND converter = ?", (key, converter)).fetchone()
                if row is not None and row[0] is not None:
                    self._writer.execute("COMMIT")
                    return {"state": "complete", "bootstrap": "sha256:" + row[0].hex()}
                if row is not None and row[1] > now and row[2] != owner:
                    self._writer.execute("COMMIT")
                    return {"state": "busy", "retry_after": min(30, row[1] - now)}
                self._writer.execute("INSERT INTO layers (diff_id, converter, created, claimed_until, owner) "
                                     "VALUES (?, ?, ?, ?, ?) ON CONFLICT(diff_id, converter) DO UPDATE SET claimed_until = "
                                     "excluded.claimed_until, owner = excluded.owner",
                                     (key, converter, now, now + seconds, owner))
                self._writer.execute("COMMIT")
                return {"state": "claimed"}
            except BaseException:
                self._writer.execute("ROLLBACK")
                raise

    def commit(self, packs, layer=None, now=None):
        """Record durable packs and complete a layer in one transaction (§3 step 5).

        ``packs`` is [(digest hex, size, footer entries)]. The first committed
        copy of a chunk wins; a condemned row moves to the new copy.
        """
        now = int(time.time() if now is None else now)
        inserted = 0
        with self._write_lock:
            self._writer.execute("BEGIN IMMEDIATE")
            try:
                for digest, size, entries in packs:
                    self._writer.execute("INSERT OR IGNORE INTO packs (digest, bytes, chunks, created, state) "
                                         "VALUES (?, ?, ?, ?, 'live')", (bytes.fromhex(digest), size, len(entries), now))
                    pack, = self._writer.execute("SELECT pack FROM packs WHERE digest = ?",
                                                 (bytes.fromhex(digest),)).fetchone()
                    before = self._writer.total_changes
                    self._writer.executemany(
                        "INSERT INTO chunks (id, pack, off, clen, ulen, flags) VALUES (?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(id) DO UPDATE SET pack = excluded.pack, off = excluded.off, clen = "
                        "excluded.clen, ulen = excluded.ulen, flags = excluded.flags, condemned = 0 "
                        "WHERE chunks.condemned != 0",
                        [(chunk_id, pack, offset, clen, ulen, flags) for chunk_id, offset, clen, ulen, flags in entries])
                    inserted += self._writer.total_changes - before
                    self._writer.executemany("DELETE FROM reservations WHERE id = ?",
                                             [(entry[0],) for entry in entries])
                self._writer.execute("DELETE FROM reservations WHERE until <= ?", (now,))
                if layer is not None:
                    diff_id, converter, bootstrap = layer
                    self._writer.execute(
                        "INSERT INTO layers (diff_id, converter, bootstrap, created, claimed_until) "
                        "VALUES (?, ?, ?, ?, 0) ON CONFLICT(diff_id, converter) DO UPDATE SET bootstrap = excluded.bootstrap, "
                        "claimed_until = 0", (bytes.fromhex(require_digest(diff_id)[7:]), converter,
                                              bytes.fromhex(require_digest(bootstrap)[7:]), now))
                self._writer.execute("COMMIT")
            except BaseException:
                self._writer.execute("ROLLBACK")
                raise
        return {"chunks_inserted": inserted}

    def register(self, component, chunk_map_digest, bootstrap_digest, chunk_map, now=None, tail_ids=()):
        """Bind a component to its chunk map once every id is live (§3 step 8).

        ``tail_ids`` are its blobs' other chunks (nydusd conversions): live
        with the root, though no chunk map entry names them.
        """
        now = int(time.time() if now is None else now)
        key = bytes.fromhex(require_digest(component)[7:])
        ids = list(dict.fromkeys((*chunk_map.ids, *tail_ids)))
        with self._write_lock:
            self._writer.execute("BEGIN IMMEDIATE")
            try:
                row = self._writer.execute("SELECT chunk_map, epoch FROM roots WHERE component = ?", (key,)).fetchone()
                if row is not None:
                    if row[0] != bytes.fromhex(chunk_map_digest[7:]):
                        raise ValueError("component is registered with another chunk map")
                    self._writer.execute("COMMIT")
                    return {"epoch": row[1]}
                rows = self._rows("SELECT id, pack, clen, condemned FROM chunks WHERE id IN ({})", ids, self._writer)
                missing = sum(1 for chunk_id in ids if chunk_id not in rows or rows[chunk_id][2])
                if missing:
                    raise MissingChunks(f"{missing} chunks of this map are unknown or condemned; reconvert")
                usage = {}
                for pack, clen, _ in rows.values():
                    usage[pack] = usage.get(pack, 0) + clen
                self._writer.execute("INSERT INTO roots VALUES (?, ?, ?, ?, 1)", (
                    key, bytes.fromhex(chunk_map_digest[7:]), bytes.fromhex(bootstrap_digest[7:]), now))
                self._writer.executemany("INSERT INTO root_packs VALUES (?, ?, ?)",
                                         [(key, pack, size) for pack, size in usage.items()])
                self._writer.execute("COMMIT")
                return {"epoch": 1}
            except BaseException:
                self._writer.execute("ROLLBACK")
                raise

    def root(self, component):
        row = self._reader().execute("SELECT chunk_map, bootstrap, epoch FROM roots WHERE component = ?",
                                     (bytes.fromhex(require_digest(component)[7:]),)).fetchone()
        return None if row is None else ("sha256:" + row[0].hex(), "sha256:" + row[1].hex(), row[2])


# --- The service ---

class ChunkIndexService:
    """API behind ChunkIndexServer; ``store`` is a ChunkObjectStore.

    With ``store_url`` (a store node, C2.6) worker locators name the node's
    URLs, which never expire; builder lookups (``locate``) stay presigned,
    since builders hold the S3 key.
    """

    def __init__(self, index, store, *, locator_cache=64, store_url=None):
        self.index, self.store, self.store_url = index, store, store_url and store_url.rstrip("/")
        self._maps, self._locators, self._guard = OrderedDict(), OrderedDict(), threading.Lock()
        self._locator_cache = locator_cache

    def _cached(self, cache, key, value=None):
        with self._guard:
            if value is None:
                if key in cache:
                    cache.move_to_end(key)
                return cache.get(key)
            cache[key] = value
            while len(cache) > self._locator_cache:
                cache.popitem(last=False)
            return value

    def chunk_map(self, digest, size=None):
        found = self._cached(self._maps, digest)
        if found is not None:
            return found
        payload = self.store.get(chunk_map_key(self.store.prefix, require_digest(digest)[7:]),
                                 size or RAFS_MAX_CHUNK_MAP_BYTES)
        if (size is not None and len(payload) != size) or content_digest(payload) != digest:
            raise ValueError("chunk map identity mismatch")
        return self._cached(self._maps, digest, ChunkMap.decode(payload))

    def commit(self, request):
        packs = []
        for item in request["packs"]:
            digest, size = require_hex(item["digest"]), item["size"]
            if type(size) is not int or not 0 < size <= MAX_PACK_BYTES:
                raise ValueError("invalid pack size")
            packs.append((digest, size, self.store.pack_entries(digest, size)))
        layer = request.get("layer")
        if layer is not None:
            layer = (layer["diff_id"], _converter(layer["converter"]), layer["bootstrap"])
            if self.store.size(bootstrap_key(self.store.prefix, require_digest(layer[2])[7:])) is None:
                raise ValueError("layer bootstrap is not durable in the object store")
        return self.index.commit(packs, layer)

    def register(self, request):
        chunk_map = request["chunk_map"]
        parsed = self.chunk_map(chunk_map["digest"], chunk_map["size"])
        bootstrap = require_digest(request["bootstrap"])
        tails = self._blob_tails(bootstrap)
        result = self.index.register(request["component"], chunk_map["digest"], bootstrap, parsed,
                                     tail_ids=[chunk for _, ids in tails for chunk in ids])
        if self.store_url is not None:
            # Runtime reads never query the index (gate run 3: 20 cold attaches
            # computing locators at once took 44 s and stalled every worker).
            for blob_id, ids in tails:
                key = blob_layout_key(self.store.prefix, blob_id)
                if self.store.size(key) is None:
                    self.store.put_bytes(key, self.locate(ids, url=self._worker_url).encode())
            self.locator(request["component"])
        return result

    def _blob_tails(self, bootstrap_digest):
        """(blob id, chunk ids) of the bootstrap's blobs that have a tail (nydusd conversions).

        A merged bootstrap omits chunks only whiteout-hidden files use, but
        nydusd and block readahead read across them, so they live with the
        root (docs/benchmarks/nydusd-spike-2026-10-03).
        """
        compressed = self.store.get(bootstrap_key(self.store.prefix, bootstrap_digest[7:]), MAX_BOOTSTRAP_BYTES)
        bootstrap = zstd_decompress(compressed, zstd_content_size(compressed, MAX_BOOTSTRAP_BYTES))
        if content_digest(bootstrap) != bootstrap_digest:
            raise ValueError("bootstrap identity mismatch")
        tails = []
        for device in parse_bootstrap(bootstrap).devices:
            key = tail_key(self.store.prefix, device[0])
            size = self.store.size(key)
            if size is not None:  # The table plus nydus's own blob metadata.
                if size > MAX_TAIL_BYTES:
                    raise ValueError("nydus blob tail exceeds its bound")
                tails.append((device[0], [entry[2] for entry in decode_tail_table(self.store.get(key, size))[0]]))
        return tails

    def _worker_url(self, key):
        if self.store_url is None:
            return self.store.url(key)
        return f"{self.store_url}/v1/objects/{key.removeprefix(self.store.prefix + '/')}"

    def locate(self, ids, meta=None, epoch=0, url=None):
        rows = self.index.locate(ids)
        if any(row is None for row in rows):
            raise MissingChunks(f"{sum(row is None for row in rows)} chunks are unknown or condemned")
        packs = list(dict.fromkeys(row[0] for row in rows))
        position = {digest: index for index, digest in enumerate(packs)}
        url = url or self.store.url
        return Locator(epoch, tuple((digest, url(pack_key(self.store.prefix, digest))) for digest in packs),
                       tuple((position[row[0]], *row[1:]) for row in rows), meta or {})

    def locator(self, component):
        root = self.index.root(component)
        if root is None:
            raise UnknownRoot("component is not registered")
        chunk_map_digest, bootstrap_digest, epoch = root
        key = (component, epoch)
        found = self._cached(self._locators, key)
        # Cached for half the URL lifetime, so served URLs stay valid for
        # at least the other half; workers refetch on 403.
        if found is not None and found[0] > time.monotonic():
            return found[1]
        meta = {"bootstrap": self._worker_url(bootstrap_key(self.store.prefix, bootstrap_digest[7:])),
                "chunk_map": self._worker_url(chunk_map_key(self.store.prefix, chunk_map_digest[7:]))}
        stored = locator_key(self.store.prefix, component[7:], epoch) if self.store_url else None
        encoded = None
        if stored is not None and (size := self.store.size(stored)) is not None:
            encoded = self.store.get(stored, size)  # Built at registration; never recomputed per attach.
        if encoded is None:
            encoded = self.locate(list(self.chunk_map(chunk_map_digest).ids), meta, epoch, self._worker_url).encode()
            if stored is not None:  # A registration from before stored locators: once.
                self.store.put_bytes(stored, encoded)
        lifetime = float("inf") if self.store_url else self.store.url_seconds / 2
        self._cached(self._locators, key, (time.monotonic() + lifetime, encoded))
        return encoded


def _converter(value):
    if not isinstance(value, str) or not 0 < len(value) <= 256 or not value.isprintable():
        raise ValueError("invalid layer converter identity")
    return value


def _claim(service, request):
    return service.index.claim_layer(request["diff_id"], _converter(request["converter"]),
                                     _converter(request["owner"]))


def _reserve(service, path, body):
    owners = parse_qs(urlsplit(path).query).get("owner", [])
    if len(owners) != 1:
        raise ValueError("reserve needs one owner")
    return bytes(service.index.reserve(_ids(body), _converter(owners[0])))


def _ids(body):
    if len(body) % 32 or len(body) // 32 > MAX_MAP_ENTRIES:
        raise ValueError("chunk id batch must be whole 32-byte ids")
    return [body[index:index + 32] for index in range(0, len(body), 32)]


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):  # noqa: A002 - BaseHTTPRequestHandler's name
        _LOG.debug("chunk index %s", format % args)

    def _authorized(self, write):
        supplied = self.headers.get("Authorization", "").removeprefix("Bearer ").encode()
        tokens = (self.server.write_token,) if write else (self.server.write_token, self.server.read_token)
        return any(token and hmac.compare_digest(supplied, token) for token in tokens)

    def _reply(self, status, payload, content_type="application/json"):
        if not isinstance(payload, bytes):
            payload = json.dumps(payload, sort_keys=True).encode()
        self.send_response(status)
        if status >= 400:
            # A refused request's body may be unread: never reuse its connection.
            self.close_connection = True
            self.send_header("Connection", "close")
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _body(self, limit):
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 <= length <= limit:
            raise ValueError("chunk index request exceeds its bound")
        return self.rfile.read(length)

    def do_GET(self):  # noqa: N802 - http.server dispatch name
        match = re.fullmatch(r"/v1/roots/(sha256:[0-9a-f]{64})/locator", self.path)
        if self.path == "/healthz":
            return self._reply(200, {"ok": True})
        if match is None:
            return self._reply(404, {"error": "unknown endpoint"})
        if not self._authorized(write=False):
            return self._reply(401, {"error": "unauthorized"})
        self._serve(lambda: self.server.service.locator(match.group(1)), "application/octet-stream")

    def do_POST(self):  # noqa: N802
        if not self._authorized(write=True):
            return self._reply(401, {"error": "unauthorized"})
        service, path = self.server.service, urlsplit(self.path).path
        routes = {
            "/v1/chunks/locate": (32 * MAX_MAP_ENTRIES, lambda body: service.locate(_ids(body)).encode()),
            "/v1/chunks/reserve": (32 * MAX_MAP_ENTRIES, lambda body: _reserve(service, self.path, body)),
            "/v1/chunks/commit": (_MAX_JSON, lambda body: service.commit(json.loads(body))),
            "/v1/layers/claim": (_MAX_JSON, lambda body: _claim(service, json.loads(body))),
            "/v1/roots/register": (_MAX_JSON, lambda body: service.register(json.loads(body))),
        }
        if path not in routes or (path != self.path and path != "/v1/chunks/reserve"):
            return self._reply(404, {"error": "unknown endpoint"})
        limit, handler = routes[path]
        try:
            body = self._body(limit)
        except ValueError as exc:
            return self._reply(413, {"error": str(exc)})
        binary = path in ("/v1/chunks/locate", "/v1/chunks/reserve")
        self._serve(lambda: handler(body), "application/octet-stream" if binary else "application/json")

    def _serve(self, call, content_type):
        try:
            return self._reply(200, call(), content_type)
        except MissingChunks as exc:
            return self._reply(409, {"error": str(exc)})
        except UnknownRoot as exc:
            return self._reply(404, {"error": str(exc)})
        except (ValueError, TypeError, KeyError) as exc:
            return self._reply(400, {"error": str(exc)})
        except (OSError, RegistryRequestError) as exc:
            _LOG.warning("chunk index request failed: %s", exc)
            return self._reply(502, {"error": "object store unavailable"})


class ChunkIndexServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 1024

    def __init__(self, address, service, *, read_token, write_token):
        read_token, write_token = (token.encode() if isinstance(token, str) else token
                                   for token in (read_token, write_token))
        if not write_token or not read_token or read_token == write_token:
            raise ValueError("the chunk index needs distinct read and write tokens")
        self.service, self.read_token, self.write_token = service, read_token, write_token
        super().__init__(address, _Handler)


class ChunkIndexClient:
    """Builders (write token) and workers (read token) reach the service here."""

    def __init__(self, base_url, token, *, timeout=60.0, request=http_request):
        self.base_url, self.timeout, self._request = base_url.rstrip("/"), timeout, request
        self._headers = {"Authorization": "Bearer " + token}

    def _call(self, method, path, body=None, *, binary=False, max_bytes=_MAX_JSON):
        headers = dict(self._headers)
        if body is not None and not binary:
            body = json.dumps(body, sort_keys=True).encode()
        headers["Content-Type"] = "application/octet-stream" if binary else "application/json"
        _, _, payload = self._request(method, self.base_url + path, headers=headers, body=body,
                                      timeout=self.timeout, max_bytes=max_bytes)
        return payload

    def locate(self, ids):
        return Locator.decode(self._call("POST", "/v1/chunks/locate", b"".join(ids), binary=True,
                                         max_bytes=64 * 1024 ** 2))

    def reserve(self, ids, owner):
        payload = self._call("POST", "/v1/chunks/reserve?owner=" + quote(owner, safe=""), b"".join(ids),
                             binary=True, max_bytes=len(ids))
        if len(payload) != len(ids) or set(payload) - {RESERVED, KNOWN, BUSY}:
            raise ValueError("invalid chunk reserve response")
        return list(payload)

    def claim_layer(self, diff_id, converter, owner):
        return json.loads(self._call("POST", "/v1/layers/claim",
                                     {"diff_id": diff_id, "converter": converter, "owner": owner}))

    def commit(self, packs, layer=None):
        return json.loads(self._call("POST", "/v1/chunks/commit", {"packs": packs, "layer": layer}))

    def register(self, component, bootstrap, chunk_map):
        return json.loads(self._call("POST", "/v1/roots/register", {
            "component": component, "bootstrap": bootstrap, "chunk_map": chunk_map}))

    def locator(self, component):
        return Locator.decode(self._call("GET", f"/v1/roots/{require_digest(component)}/locator",
                                         max_bytes=64 * 1024 ** 2))
