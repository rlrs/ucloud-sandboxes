"""The worker's RAFS device: a chunk-store image as one verified block device.

Read path of docs/chunk-store-design.md §4. The bootstrap is served from
memory, a block resolves to a chunk id through the signed chunk map, and the
unsigned locator says which pack range holds it. Misses fetch 1 MiB windows
(prefetch up to 4 MiB) with plain HTTP ranges, from S3 or, when configured,
only from the store node (C2.6); every chunk is decompressed
and checked against its id before it is cached or served. The cache is keyed
by chunk id, so images share chunks. C2.1's Rust device implements the same
contract; this is the reference.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import dataclasses
import hashlib
import os
from pathlib import Path
import threading

from .chunk_index import http_range, http_request
from .chunk_store import (BLOCK, RAW, ChunkMap, bootstrap_devices, chunk_map_regions, decode_chunk, pack_key,
                          parse_bootstrap, zstd_decompress)
from .environment_artifact import Chunk, RafsEnvironmentComponent, content_digest
from .managed_registry import RegistryRequestError

DEMAND_WINDOW_BYTES = 1024 ** 2
PREFETCH_RANGE_BYTES = 4 * 1024 ** 2
MERGE_GAP_BYTES = 64 * 1024


class VerifiedBootstrap:
    """The verified bootstrap, served from memory or from a private file
    checked block by block: at a median 8 MB, 500 mounted images would
    otherwise hold 4 GB of backend memory."""

    def __init__(self, data, path=None):
        self.size, self.path, self._data, self._fd = len(data), path, data, None
        if path is not None:
            self._hashes = [hashlib.sha256(data[offset:offset + BLOCK]).digest() for offset in range(0, len(data), BLOCK)]
            temporary = path.with_name(path.name + f".{os.getpid()}.{threading.get_ident()}")
            with open(temporary, "wb") as stream:
                stream.write(data)
            os.replace(temporary, path)
            self._fd, self._data = os.open(path, os.O_RDONLY | os.O_NOFOLLOW), None

    def read(self, offset, length):
        if self._data is not None:
            return self._data[offset:offset + length]
        first, last = offset // BLOCK, (offset + length - 1) // BLOCK
        raw = os.pread(self._fd, (last - first + 1) * BLOCK, first * BLOCK)
        for position, block in enumerate(range(first, last + 1)):
            if hashlib.sha256(raw[position * BLOCK:(position + 1) * BLOCK]).digest() != self._hashes[block]:
                raise ValueError("RAFS bootstrap file changed after verification")
        return raw[offset - first * BLOCK:offset - first * BLOCK + length]

    def close(self):
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
            self.path.unlink(missing_ok=True)


class RafsImage:
    """A verified chunk-store image, shaped like a component for the cache,
    the NBD export and the trace store (``image_digest``, ``image_size``,
    ``chunks``)."""

    def __init__(self, digest, component, bootstrap, chunk_map, locator, *, refresh=None, reader=http_range,
                 origin=None):
        if not isinstance(component, RafsEnvironmentComponent) or chunk_map.device_size != component.device_size:
            raise ValueError("RAFS chunk map does not match its signed component")
        self.origin = origin  # With a store node, every locator URL must name it.
        if not isinstance(bootstrap, VerifiedBootstrap):
            bootstrap = VerifiedBootstrap(bootstrap)
        self.digest, self.component, self.bootstrap, self.map = digest, component, bootstrap, chunk_map
        self.image_digest, self.image_size = component.image_digest, component.device_size
        self.chunks = tuple(Chunk("sha256:" + chunk_id.hex(), size) for chunk_id, size in zip(chunk_map.ids, chunk_map.sizes))
        self.chunk_ids = tuple(chunk.digest[7:] for chunk in self.chunks)
        self._first = {}
        for index, chunk in enumerate(self.chunks):
            self._first.setdefault(chunk.digest, index)
        self._refresh, self.reader, self._guard = refresh, reader, threading.Lock()
        self._use(locator)

    def authenticate(self, trusted_keys):
        self.component.authenticate(trusted_keys)
        return self

    def close(self):
        self.bootstrap.close()

    def chunk_index(self, chunk_id):
        return self._first.get("sha256:" + chunk_id)

    def _use(self, locator):
        """Adopt a locator after bounds checks against the verified map."""
        require_origin(locator, self.origin)
        if len(locator.entries) != len(self.chunks) or any(
                flags == RAW and clen != size for (_, _, clen, flags), size in zip(locator.entries, self.map.sizes)):
            raise ValueError("chunk locator does not fit its chunk map")
        order = {}
        for index, (pack, offset, clen, _flags) in enumerate(locator.entries):
            if self._first[self.chunks[index].digest] == index:
                order.setdefault(pack, []).append((offset, clen, index))
        for entries in order.values():
            entries.sort()
        with self._guard:
            self._locator, self._order = locator, order
            self._position = {index: (pack, position) for pack, entries in order.items()
                              for position, (_, _, index) in enumerate(entries)}

    def refresh(self, stale):
        """One locator refetch per stale locator, shared by concurrent failures."""
        with self._guard:
            if self._locator is not stale or self._refresh is None:
                return self._refresh is not None
        self._use(self._refresh())
        return True

    def prefetch_order(self, indices):
        """Trace replay order: by pack, then offset, so ranges coalesce."""
        with self._guard:
            entries = self._locator.entries
        unique = dict.fromkeys(self._first[self.chunks[index].digest] for index in indices
                               if type(index) is int and 0 <= index < len(self.chunks))
        return tuple(sorted(unique, key=lambda index: entries[index][:2]))

    def _span(self, members, entries):
        start = min(entries[index][1] for index in members)
        end = max(entries[index][1] + entries[index][2] for index in members)
        return start, end - start

    def _get(self, locator, pack, members, timeout):
        """{chunk digest: verified bytes} of one range over ``members``."""
        entries = locator.entries
        start, length = self._span(members, entries)
        payload = self.reader(locator.packs[pack][1], start, length, timeout=timeout)
        verified = {}
        for index in members:
            _, offset, clen, flags = entries[index]
            chunk = self.chunks[index]
            try:
                verified[chunk.digest] = decode_chunk(payload[offset - start:offset - start + clen], chunk.size,
                                                      flags, bytes.fromhex(chunk.digest[7:]))
            except ValueError:
                pass
        return verified

    def window(self, index, cached, limit=DEMAND_WINDOW_BYTES):
        """Members of the demand window around ``index`` in its pack: read
        ahead, then behind, over uncached neighbours within ``limit``."""
        index = self._first[self.chunks[index].digest]
        with self._guard:
            locator, (pack, position) = self._locator, self._position[index]
            order = self._order[pack]
        low = high = position
        start, end = order[position][0], order[position][0] + order[position][1]
        while high + 1 < len(order):
            offset, clen, neighbour = order[high + 1]
            if offset - end >= MERGE_GAP_BYTES or offset + clen - start > limit or cached(self.chunks[neighbour]):
                break
            high, end = high + 1, max(end, offset + clen)
        while low > 0:
            offset, clen, neighbour = order[low - 1]
            if start - offset - clen >= MERGE_GAP_BYTES or end - offset > limit or cached(self.chunks[neighbour]):
                break
            low, start = low - 1, offset
        return locator, pack, [item[2] for item in order[low:high + 1]]

    def fetch_window(self, cache, chunk, timeout):
        """Demand miss: one GET for the window; siblings are installed too.

        A 403 (expired URL) or a chunk that does not verify refetches the
        locator once; then the read fails with EIO, never zeros.
        """
        index = self._first[chunk.digest]
        for attempt in (0, 1):
            locator, pack, members = self.window(index, cache.contains)
            futures = cache.join_window([self.chunks[member] for member in members if member != index])
            verified = {}
            try:
                verified = self._get(locator, pack, members, timeout)
            except RegistryRequestError as exc:
                if exc.status_code != 403 or attempt or not self.refresh(locator):
                    raise
                continue
            finally:
                cache.finish_window(futures, verified)
            if chunk.digest in verified:
                return verified[chunk.digest]
            cache.count_corruption()
            if attempt or not self.refresh(locator):
                break
        raise ValueError("chunk store bytes do not verify against the chunk id")

    def next_run(self, queue, busy, limit=PREFETCH_RANGE_BYTES):
        """Pop one prefetch run off ``queue`` (reversed, as PrefetchJob keeps
        it): same pack, gaps under 64 KiB, within ``limit``; returns
        (run of indices, skipped count)."""
        with self._guard:
            entries = self._locator.entries
        run, skipped = [], 0
        while queue:
            index = queue[-1]
            pack, offset, clen, _ = entries[index]
            if run:
                first, last = entries[run[0]], entries[run[-1]]
                if pack != first[0] or offset - (last[1] + last[2]) >= MERGE_GAP_BYTES \
                        or offset + clen - first[1] > limit or offset < last[1] + last[2]:
                    break
            queue.pop()
            if busy(self.chunks[index]):
                skipped += 1
                continue
            run.append(index)
        return run, skipped

    def fetch_run(self, run, timeout, limit=PREFETCH_RANGE_BYTES):
        """Prefetch: one GET, no retries; {digest: verified bytes}. A locator
        refreshed since planning may split the run: then nothing is fetched."""
        with self._guard:
            locator = self._locator
        packs = {locator.entries[index][0] for index in run}
        if len(packs) != 1 or self._span(run, locator.entries)[1] > limit:
            return {}
        return self._get(locator, packs.pop(), run, timeout)


def _get(url, limit, timeout=60.0):
    return http_request("GET", url, timeout=timeout, max_bytes=limit)[2]


def store_prefix(base_url):
    return base_url.rstrip("/") + "/v1/objects/"


def store_locator(locator, base_url, prefix):
    """``locator`` with its packs named on the store node, as the index names them for workers."""
    packs = tuple((digest, store_prefix(base_url) + pack_key(prefix, digest).removeprefix(prefix + "/"))
                  for digest, _ in locator.packs)
    return dataclasses.replace(locator, packs=packs)


def require_origin(locator, origin):
    """With a store node configured, a locator naming anything else (S3, a
    presigned URL) is refused: workers fail closed, never reach S3."""
    if origin is not None and any(not url.startswith(store_prefix(origin))
                                  for url in [url for _, url in locator.packs] + list(locator.meta.values())):
        raise ValueError("chunk locator names a source other than the configured store node")


def store_access(base_url, token):
    """(reader, getter) that read only from the store node, with its token."""
    prefix, headers = store_prefix(base_url), {"Authorization": "Bearer " + token}

    def check(url):
        if not url.startswith(prefix):
            raise ValueError("refusing a chunk read outside the configured store node")
        return url

    def reader(url, start, length, *, timeout=30.0):
        return http_range(check(url), start, length, timeout=timeout, headers=headers)

    def getter(url, limit, timeout=60.0):
        return http_request("GET", check(url), headers=headers, timeout=timeout, max_bytes=limit)[2]
    return reader, getter


def load_rafs_image(digest, component, index, *, getter=_get, reader=http_range, meta_root=None, origin=None):
    """Fetch and verify the bootstrap and chunk map (design §1.5 steps 3-4);
    ``meta_root``, a private directory, keeps bootstraps out of memory;
    ``origin``, a store node's URL, is the only source a locator may name."""
    locator = index.locator(digest)
    if set(locator.meta) != {"bootstrap", "chunk_map"}:
        raise ValueError("chunk locator lacks the image metadata URLs")
    require_origin(locator, origin)
    boot_size, map_size = component.bootstrap["size"], component.chunk_map["size"]
    with ThreadPoolExecutor(2, thread_name_prefix="rafs-meta") as pool:
        compressed = pool.submit(getter, locator.meta["bootstrap"], boot_size + boot_size // 64 + (1 << 20))
        encoded = pool.submit(getter, locator.meta["chunk_map"], map_size)
        compressed, encoded = compressed.result(), encoded.result()
    bootstrap = zstd_decompress(compressed, boot_size)
    if "sha256:" + hashlib.sha256(bootstrap).hexdigest() != component.bootstrap["digest"]:
        raise ValueError("RAFS bootstrap identity mismatch")
    if len(encoded) != map_size or content_digest(encoded) != component.chunk_map["digest"]:
        raise ValueError("RAFS chunk map identity mismatch")
    chunk_map = ChunkMap.decode(encoded)
    if not chunk_map.matches(parse_bootstrap(bootstrap)):
        raise ValueError("RAFS chunk map regions differ from the bootstrap's device table")
    if meta_root is not None:
        bootstrap = VerifiedBootstrap(bootstrap, Path(meta_root) / (digest[7:] + ".boot"))
    return RafsImage(digest, component, bootstrap, chunk_map, locator, refresh=lambda: index.locator(digest),
                     reader=reader, origin=origin)


class NydusdImage:
    """A verified chunk-store image as nydusd reads it: the bootstrap in a
    private file and the blob regions, nothing per chunk (nydusd checks every
    chunk against the bootstrap's digests). Building RafsImage's per-chunk
    state took seconds of Python per large image, and a burst's attaches
    shared one GIL: 8 s each under load against 0.5 s alone (2026-10-04)."""

    def __init__(self, digest, component, path, regions):
        self.digest, self.component, self.path = digest, component, path
        self.bootstrap = self  # The factory reads ``bootstrap.path``.
        self.map = dataclasses.make_dataclass("Regions", ["regions"])(regions)
        self.image_digest, self.image_size = component.image_digest, component.device_size

    def authenticate(self, trusted_keys):
        self.component.authenticate(trusted_keys)
        return self

    def close(self):
        self.path.unlink(missing_ok=True)


def load_nydusd_image(digest, component, *, getter, meta_root, origin):
    """Fetch and verify the bootstrap and chunk map from the store node, by
    the digests the signed component pins (no index: their keys follow from
    the digests), and check the map's regions against the bootstrap's devices."""
    boot, chunk_map = component.bootstrap, component.chunk_map
    base = store_prefix(origin) + "meta/"
    with ThreadPoolExecutor(2, thread_name_prefix="rafs-meta") as pool:
        compressed = pool.submit(getter, base + boot["digest"][7:] + ".boot.zst", boot["size"] + boot["size"] // 64
                                 + (1 << 20))
        encoded = pool.submit(getter, base + chunk_map["digest"][7:] + ".map", chunk_map["size"])
        compressed, encoded = compressed.result(), encoded.result()
    bootstrap = zstd_decompress(compressed, boot["size"])
    if content_digest(bootstrap) != boot["digest"]:
        raise ValueError("RAFS bootstrap identity mismatch")
    if len(encoded) != chunk_map["size"] or content_digest(encoded) != chunk_map["digest"]:
        raise ValueError("RAFS chunk map identity mismatch")
    bootstrap_size, device_size, regions = chunk_map_regions(encoded)
    if (bootstrap_size != len(bootstrap) or device_size != component.device_size or sorted(regions) != sorted(
            (blob, mapped, blocks) for blob, blocks, mapped in bootstrap_devices(bootstrap))):
        raise ValueError("RAFS chunk map regions differ from the bootstrap's device table")
    path = Path(meta_root) / (digest[7:] + ".boot")
    temporary = path.with_name(path.name + f".{os.getpid()}.{threading.get_ident()}")
    temporary.write_bytes(bootstrap)
    os.replace(temporary, path)
    return NydusdImage(digest, component, path, regions)


def read_image(cache, image, offset, length, cancel=None):
    """Block read over the unified device space; holes read as zeros."""
    end, output = offset + length, bytearray(length)
    if offset < image.bootstrap.size:
        piece = image.bootstrap.read(offset, min(end, image.bootstrap.size) - offset)
        output[:len(piece)] = piece
    starts = image.map.offsets
    for index in image.map.overlapping(offset, end):
        cache.observe(image, index)
        data = cache.chunk(image.chunks[index], cancel=cancel, source=image)
        start = starts[index]
        low, high = max(offset, start), min(end, start + len(data))
        output[low - offset:high - offset] = data[low - start:high - start]
    return bytes(output)

