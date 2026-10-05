"""``ucloud-chunk-store``: the store node's read-through NVMe cache over S3.

Phase B of docs/chunk-store-design.md (C2.6, S12). Workers range-read packs,
bootstraps and chunk maps here on the private network with the index's read
token; they never hold S3 credentials or URLs. A miss fills one aligned
extent from S3 with the node's own key (M1's SigV4 presigning), coalescing
concurrent misses and hedging requests whose bytes are late, since S3's tail
is seconds (S12). Cache hits go to the socket with sendfile(2).

The node is untrusted, like S3: workers verify every chunk. It still never
keeps bytes it can prove wrong (whole packs and chunk maps are named by their
sha256), and a cached extent carries its own sha256 in its file name, so an
extent torn by power loss fails its check on first use and is refetched.
"""
from __future__ import annotations

import asyncio
import errno
from collections import OrderedDict, deque
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
import hashlib
import hmac
from http import HTTPStatus
import itertools
import json
import logging
import os
from pathlib import Path
import queue
import re
import secrets
import shutil
import socket
import stat
import threading
import time

from .chunk_index import http_request
from .managed_registry import RegistryRequestError

_LOG = logging.getLogger(__name__)
MIB = 1024 ** 2
MAX_EXTENT_BYTES = 64 * MIB  # A whole pack.
MAX_RESPONSE_BYTES = 256 * MIB  # Bounds one request; bootstraps are at most 128 MiB.
FILL_DEADLINE_SECONDS = 60.0
ATTEMPT_READ_TIMEOUT_SECONDS = 20.0
# Hedge a GET once no attempt has made progress (a first byte, then body
# bytes) for 3 x the median time to first byte, within [150 ms, 2 s]; at most
# twice. S12: stalls, before the first byte or mid-body, carry the tail. A
# slow but flowing transfer is never duplicated: in a bandwidth-bound burst
# that only halves everyone's share (docs/benchmarks/chunk-store-node-2026-10-02).
HEDGE_MIN_SECONDS, HEDGE_MAX_SECONDS, MAX_HEDGES = 0.15, 2.0, 2
READ_BLOCK = 256 * 1024  # Progress granularity of a fill.
_RETRYABLE = {408, 429, 500, 502, 503, 504}
# Only the chunk store's content-addressed objects; a read token reads nothing else.
_KEY = re.compile(r"packs/([0-9a-f]{2})/([0-9a-f]{64})\.pack|meta/([0-9a-f]{64})\.(boot\.zst|map|tail|layout)")
_KINDS = {"pack": "packs/{0}/{1}.pack", "boot": "meta/{1}.boot.zst", "map": "meta/{1}.map",
          "tail": "meta/{1}.tail", "layout": "meta/{1}.layout"}
# nydusd's registry backend: GET/HEAD /v2/<repository>/blobs/sha256:<blob id>.
_VIRTUAL = re.compile(r"/v2/virtual/([0-9a-f]{64})/blobs/sha256:([0-9a-f]{64})")
_FILE = re.compile(r"([0-9a-f]{64})\.(pack|boot|map|tail|layout)\.(\d{1,6})\.(\d{1,12})\.([0-9a-f]{64})")


def object_identity(relative):
    """(digest, kind) of an object key under the chunk-store prefix."""
    match = _KEY.fullmatch(relative) if isinstance(relative, str) else None
    if match is None or (match.group(1) and match.group(1) != match.group(2)[:2]):
        raise LookupError("not a chunk store object")
    if match.group(2):
        return match.group(2), "pack"
    return match.group(3), {"boot.zst": "boot", "map": "map", "tail": "tail", "layout": "layout"}[match.group(4)]


def object_key(digest, kind):
    return _KINDS[kind].format(digest[:2], digest)


class NotFound(LookupError):
    pass


class RangeNotSatisfiable(ValueError):
    def __init__(self, total):
        super().__init__("range is not satisfiable")
        self.total = total


class _Retry(OSError):
    pass


class Samples:
    """Recent values for percentiles, and a running count."""

    def __init__(self, size=2048):
        self._values, self._guard, self.count = deque(maxlen=size), threading.Lock(), 0

    def add(self, value):
        with self._guard:
            self._values.append(value)
            self.count += 1

    def percentile(self, fraction, default=None):
        with self._guard:
            ordered = sorted(self._values)
        return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))] if ordered else default

    def summary(self):
        with self._guard:
            ordered, count = sorted(self._values), self.count
        pick = lambda q: round(ordered[min(len(ordered) - 1, int(q * len(ordered)))] * 1000, 1) if ordered else None  # noqa: E731
        return {"count": count, "p50_ms": pick(.5), "p90_ms": pick(.9), "p99_ms": pick(.99),
                "max_ms": round(ordered[-1] * 1000, 1) if ordered else None}


class Extent:
    __slots__ = ("path", "size", "total", "sha256", "verified")

    def __init__(self, path, size, total, sha256, verified):
        self.path, self.size, self.total, self.sha256, self.verified = path, size, total, sha256, verified


class ExtentCache:
    """Extents on local disk under a byte budget, least recently used first.

    A file appears under its final name only once complete (written in
    ``tmp/``, then renamed), and the name carries its sha256. Nothing is
    fsynced: after a restart every extent is hashed on first use, so one torn
    by power loss is dropped and refetched, never served.
    """

    def __init__(self, root, budget, replica=False):
        if budget < MIB:
            raise ValueError("the chunk store cache budget must hold an extent")
        # A replica keeps everything: past its budget it refuses new extents
        # (counted) instead of evicting; the operator grows the disk.
        self.root, self.budget, self.replica, self.full_refusals = Path(root), budget, replica, 0
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.tmp = self.root / "tmp"
        shutil.rmtree(self.tmp, ignore_errors=True)  # Fills a crash interrupted.
        self.tmp.mkdir(mode=0o700)
        self._guard = threading.Lock()
        self._lru, self._totals, self.bytes, self.reserved = OrderedDict(), {}, 0, 0
        self.evictions = self.verify_failures = 0
        found = []
        for directory in self.root.iterdir():
            if directory.name == "tmp" or not directory.is_dir():
                continue
            for path in directory.iterdir():
                match = _FILE.fullmatch(path.name)
                info = path.lstat()
                if (match is None or match.group(1)[:2] != directory.name or not stat.S_ISREG(info.st_mode)
                        or info.st_size <= 0):
                    path.unlink(missing_ok=True)
                    continue
                digest, kind, index, total, sha = match.groups()
                found.append((info.st_mtime_ns, (digest, kind, int(index)), Extent(path, info.st_size, int(total),
                                                                                    sha, False)))
        for _, ident, extent in sorted(found, key=lambda item: item[0]):
            self._lru[ident] = extent
            self._totals[ident[:2]] = extent.total
            self.bytes += extent.size
        self._evict()

    def total(self, digest, kind):
        with self._guard:
            return self._totals.get((digest, kind))

    def contains(self, ident):
        with self._guard:
            return ident in self._lru

    def get(self, ident):
        with self._guard:
            return self._lru.get(ident)

    def open(self, ident):
        """A readable file of a verified extent (hashed on its first open since
        the restart), or None (absent or torn)."""
        with self._guard:
            extent = self._lru.get(ident)
            if extent is None:
                return None
            self._lru.move_to_end(ident)
        try:
            stream = open(extent.path, "rb", buffering=0)
        except FileNotFoundError:
            self._drop(ident, extent)
            return None
        if not extent.verified:
            digest, size = hashlib.sha256(), 0
            while block := stream.read(MIB):
                digest.update(block)
                size += len(block)
            if size != extent.size or digest.hexdigest() != extent.sha256:
                stream.close()
                with self._guard:
                    self.verify_failures += 1
                self._drop(ident, extent)
                return None
            extent.verified = True
        return stream, extent

    def _drop(self, ident, extent):
        with self._guard:
            if self._lru.get(ident) is extent:
                del self._lru[ident]
                self.bytes -= extent.size
        extent.path.unlink(missing_ok=True)

    def remove(self, digest, kind):
        with self._guard:
            idents = [ident for ident in self._lru if ident[:2] == (digest, kind)]
        for ident in idents:
            extent = self._lru.get(ident)
            if extent is not None:
                self._drop(ident, extent)
        return len(idents)

    def reserve(self, size):
        with self._guard:
            if self.replica and self.bytes + self.reserved + size > self.budget:
                self.full_refusals += 1
                raise OSError(errno.ENOSPC, "chunk store replica is full: grow cache_bytes and its disk")
            self.reserved += size
            self._evict()

    def release(self, size):
        with self._guard:
            self.reserved -= size

    def temporary(self):
        name = self.tmp / f"{os.getpid()}-{threading.get_ident()}-{secrets.token_hex(8)}"
        return os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), name

    def install(self, ident, temporary, size, total, sha256):
        """Rename a complete, hashed file into place (one reservation of ``size`` ends)."""
        directory = self.root / ident[0][:2]
        directory.mkdir(mode=0o700, exist_ok=True)
        path = directory / f"{ident[0]}.{ident[1]}.{ident[2]}.{total}.{sha256}"
        os.replace(temporary, path)
        extent = Extent(path, size, total, sha256, True)
        with self._guard:
            self.reserved -= size
            previous = self._lru.pop(ident, None)
            if previous is not None:
                self.bytes -= previous.size
                if previous.path != path:
                    previous.path.unlink(missing_ok=True)
            self._lru[ident] = extent
            self._totals[ident[:2]] = total
            self.bytes += size
            self._evict()
        return extent

    def _evict(self):
        if self.replica:
            return
        # Open files keep their bytes, so a reader mid-sendfile is unaffected.
        while self.bytes + self.reserved > self.budget and self._lru:
            _, extent = self._lru.popitem(last=False)
            self.bytes -= extent.size
            self.evictions += 1
            extent.path.unlink(missing_ok=True)

    def stats(self):
        with self._guard:
            return {"bytes": self.bytes, "budget": self.budget, "extents": len(self._lru),
                    "reserved": self.reserved, "evictions": self.evictions,
                    "verify_failures": self.verify_failures, "replica": self.replica,
                    "full_refusals": self.full_refusals}


class _Race:
    """The attempts of one hedged GET: the first complete one wins."""

    def __init__(self):
        self.results, self.cancel, self.guard, self.done, self.responses = queue.Queue(), threading.Event(), \
            threading.Lock(), False, []
        self.slots = []  # Release callbacks: a decided race frees its losers' S3 slots.
        self.progress = {}  # Running attempt -> when it last received anything.

    def stalled_for(self):
        with self.guard:
            return time.monotonic() - max(self.progress.values(), default=time.monotonic())

    def slot(self, release):
        def once():
            with self.guard:
                if not state:
                    return
                state.pop()
            release()
        state = [True]
        with self.guard:
            self.slots.append(once)
        return once

    def finish(self, result, error):
        with self.guard:
            if self.done and result is not None:
                Path(result[0]).unlink(missing_ok=True)  # A late winner's bytes are not needed.
                return
            self.results.put((result, error))

    def close(self):
        with self.guard:
            self.done = True
            responses, slots = list(self.responses), list(self.slots)
        self.cancel.set()
        for release in slots:
            release()
        while True:  # Results that arrived after the winner.
            try:
                result, _ = self.results.get_nowait()
            except queue.Empty:
                break
            if result is not None:
                Path(result[0]).unlink(missing_ok=True)
        for response in responses:  # Unblock stalled reads of the losers.
            sock = getattr(getattr(response, "connection", None), "sock", None)
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except (AttributeError, OSError):
                pass


class S3Source:
    """Ranged GETs of one bucket prefix with the node's own key.

    Each attempt is a fresh SigV4 presigned GET (M1's ``S3Presigner``) on a
    keep-alive pool. Late attempts are hedged, failures retried with backoff
    until the fill deadline; at most ``concurrency`` attempts are in flight.
    """

    def __init__(self, presigner, prefix, *, concurrency=64, url_seconds=900, deadline=FILL_DEADLINE_SECONDS):
        import urllib3
        self.presigner, self.prefix, self.url_seconds, self.deadline = presigner, prefix.strip("/"), url_seconds, deadline
        self.concurrency = concurrency
        self._slots = threading.BoundedSemaphore(concurrency)
        self._pool = urllib3.PoolManager(num_pools=4, maxsize=concurrency * 2, block=False, retries=False)
        self.ttfb, self.latency = Samples(), Samples()
        self._guard = threading.Lock()
        self.counters = dict.fromkeys(("requests", "bytes", "errors", "retries", "hedged", "hedge_wins"), 0)

    def _count(self, name, value=1):
        with self._guard:
            self.counters[name] += value

    def hedge_after(self):
        return min(HEDGE_MAX_SECONDS, max(HEDGE_MIN_SECONDS, 3 * self.ttfb.percentile(.5, HEDGE_MIN_SECONDS)))

    def fetch(self, relative, start, length, cache, *, hedge=True):
        """(temporary path, sha256 hex, size, object size) of [start, start + length)."""
        deadline, backoff, started = time.monotonic() + self.deadline, .1, time.monotonic()
        while True:
            try:
                result = self._hedged(relative, start, length, cache, deadline, MAX_HEDGES if hedge else 0)
                self.latency.add(time.monotonic() - started)
                return result
            except _Retry as exc:
                if time.monotonic() + backoff >= deadline:
                    raise TimeoutError(f"S3 fill did not complete: {exc}") from exc
                self._count("retries")
                time.sleep(backoff)
                backoff = min(2.0, backoff * 2)

    def _hedged(self, relative, start, length, cache, deadline, max_hedges=MAX_HEDGES):
        race, pending, hedges, errors = _Race(), 0, 0, []
        if not self._slots.acquire(timeout=max(0.0, deadline - time.monotonic())):
            raise _Retry("no S3 request slot")
        try:
            self._launch(race, 0, relative, start, length, cache)
            pending, threshold = 1, self.hedge_after()
            while pending:
                now = time.monotonic()
                if now >= deadline:
                    raise _Retry("S3 attempts exceeded the fill deadline")
                wait = min(deadline - now, threshold / 4 if hedges < max_hedges else deadline - now)
                try:
                    result, error = race.results.get(timeout=max(0.001, wait))
                except queue.Empty:
                    # Never wait for a hedge slot.
                    if hedges < max_hedges and race.stalled_for() >= threshold and self._slots.acquire(blocking=False):
                        hedges += 1
                        pending += 1
                        self._count("hedged")
                        self._launch(race, hedges, relative, start, length, cache)
                    continue
                pending -= 1
                if result is not None:
                    if result[4]:
                        self._count("hedge_wins")
                    return result[:4]
                errors.append(error)
                if not isinstance(error, _Retry):
                    raise error
            raise errors[-1]
        finally:
            race.close()

    def _launch(self, race, number, relative, start, length, cache):
        release = race.slot(self._slots.release)
        with race.guard:
            race.progress[number] = time.monotonic()

        def run():
            try:
                race.finish(self._attempt(race, number, relative, start, length, cache) + (number > 0,), None)
            except BaseException as exc:  # noqa: BLE001 - handed to the waiting fill
                race.finish(None, exc)
            finally:
                with race.guard:
                    race.progress.pop(number, None)
                release()
        threading.Thread(target=run, name="chunk-store-s3", daemon=True).start()

    def _attempt(self, race, number, relative, start, length, cache):
        import urllib3
        url = self.presigner.url(f"{self.prefix}/{relative}", expires=self.url_seconds)
        fd, path = cache.temporary()
        cache.reserve(length)
        response, complete, began = None, False, time.monotonic()
        self._count("requests")
        try:
            try:
                response = self._pool.request(
                    "GET", url, headers={"Range": f"bytes={start}-{start + length - 1}"}, preload_content=False,
                    timeout=urllib3.Timeout(connect=5.0, read=ATTEMPT_READ_TIMEOUT_SECONDS))
            except urllib3.exceptions.HTTPError as exc:
                raise _Retry(f"S3 request failed: {type(exc).__name__}") from exc
            with race.guard:
                race.responses.append(response)
                race.progress[number] = time.monotonic()
            self.ttfb.add(time.monotonic() - began)
            match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", str(response.headers.get("Content-Range") or ""))
            if response.status == 416:
                total = re.fullmatch(r"bytes \*/(\d+)", str(response.headers.get("Content-Range") or ""))
                raise RangeNotSatisfiable(int(total.group(1)) if total else None)
            if response.status == 404:
                raise NotFound("object is not in the chunk store bucket")
            if response.status in _RETRYABLE:
                raise _Retry(f"S3 answered {response.status}")
            if response.status != 206 or match is None or int(match.group(1)) != start:
                raise RegistryRequestError(response.status, "GET", relative, "S3 did not serve the range")
            total = int(match.group(3))
            expected = min(length, total - start)
            if int(match.group(2)) != start + expected - 1:
                raise _Retry("S3 served another range")
            digest, written = hashlib.sha256(), 0
            try:
                while not race.cancel.is_set():
                    block = response.read(READ_BLOCK)
                    if not block:
                        break
                    with race.guard:
                        race.progress[number] = time.monotonic()
                    written += len(block)
                    if written > expected:
                        raise _Retry("S3 served more than the range")
                    view = memoryview(block)
                    while view:
                        view = view[os.write(fd, view):]
                    digest.update(block)
            except (urllib3.exceptions.HTTPError, OSError) as exc:
                if race.cancel.is_set():
                    raise _Retry("attempt lost its race") from exc
                raise _Retry(f"S3 body failed: {type(exc).__name__}") from exc
            if race.cancel.is_set():
                raise _Retry("attempt lost its race")
            if written != expected:
                raise _Retry("S3 body ended early")
            complete = True
            self._count("bytes", written)
            return str(path), digest.hexdigest(), written, total
        except BaseException as exc:
            if not isinstance(exc, (NotFound, RangeNotSatisfiable)) and not race.cancel.is_set():
                self._count("errors")
            Path(path).unlink(missing_ok=True)
            raise
        finally:
            cache.release(length)
            os.close(fd)
            if response is not None:
                with race.guard:  # A pooled connection must not be shut down later.
                    race.responses.remove(response) if response in race.responses else None
                response.release_conn() if complete else response.close()


class WarmJob:
    """Fill a list of objects or ranges ahead of a burst (design C9.3)."""

    def __init__(self, identifier, items, concurrency):
        self.id, self.items, self.concurrency = identifier, items, concurrency
        self.started, self.seconds, self.state = time.monotonic(), None, "running"
        self.counts = dict.fromkeys(("extents", "done", "cached", "failed", "bytes"), 0)
        self.errors, self.guard = [], threading.Lock()

    def progress(self):
        with self.guard:
            return {"job": self.id, "state": self.state, "objects": len(self.items), **self.counts,
                    "seconds": round(self.seconds if self.seconds is not None else time.monotonic() - self.started, 3),
                    "errors": self.errors[:8]}


class ChunkStoreNode:
    """Single-flight extent fills over an ExtentCache and an S3Source."""

    mirror = None  # A replica node's ReplicaMirror, for metrics.

    def __init__(self, cache, source, *, extent_bytes=8 * MIB, warm_concurrency=16):
        if extent_bytes & (extent_bytes - 1) or not MIB <= extent_bytes <= MAX_EXTENT_BYTES:
            raise ValueError("chunk store extents must be a power of two from 1 to 64 MiB")
        self.cache, self.source, self.extent_bytes = cache, source, extent_bytes
        self._fills = ThreadPoolExecutor(max_workers=source.concurrency, thread_name_prefix="chunk-store-fill")
        # Builders' reads and warm jobs (the write token) hold at most half the
        # S3 slots, unhedged: a converter verifying large images starved
        # worker fills past the NBD timeout (M2 wave 1).
        self._background_fills = ThreadPoolExecutor(max_workers=max(1, source.concurrency // 2),
                                                    thread_name_prefix="chunk-store-background")
        self._warm_slots = threading.BoundedSemaphore(warm_concurrency)
        self._guard, self._inflight, self._jobs, self._ids = threading.Lock(), {}, OrderedDict(), itertools.count(1)
        self.fill_wait = Samples()
        # A served read's wait for a read thread, its read (fills, layout, page faults) and its send.
        self.serve = {"queue": Samples(), "read": Samples(), "send": Samples()}
        self.counters = dict.fromkeys(("requests", "hits", "misses", "coalesced", "bytes_served", "errors",
                                       "fills", "fill_rejects", "warm_jobs"), 0)

    def count(self, name, value=1):
        with self._guard:
            self.counters[name] += value

    def extent(self, ident, background=False):
        """A Future of the installed extent ``ident`` = (digest, kind, index)."""
        with self._guard:
            future = self._inflight.get(ident)
            if future is not None:
                self.counters["coalesced"] += 1
                return future
            future = self._inflight[ident] = Future()
        existing = self.cache.get(ident)
        if existing is not None:  # Installed since the caller looked.
            self._settle(ident, future, existing, None)
            return future
        try:
            (self._background_fills if background else self._fills).submit(self._fill, ident, future, background)
        except RuntimeError as exc:  # Closing: nobody may wait on a fill that never runs.
            self._settle(ident, future, None, exc)
        return future

    def _settle(self, ident, future, value, error):
        with self._guard:
            self._inflight.pop(ident, None)
        future.set_exception(error) if error is not None else future.set_result(value)

    def _fill(self, ident, future, background=False):
        digest, kind, index = ident
        start, total = index * self.extent_bytes, self.cache.total(digest, kind)
        try:
            if total is not None and start >= total:
                raise RangeNotSatisfiable(total)
            length = self.extent_bytes if total is None else min(self.extent_bytes, total - start)
            path, sha256, size, total = self.source.fetch(object_key(digest, kind), start, length, self.cache,
                                                          hedge=not background)
            try:
                # Whole packs and chunk maps are named by their sha256.
                if start == 0 and size == total and kind in ("pack", "map") and sha256 != digest:
                    self.count("fill_rejects")
                    raise RegistryRequestError(502, "GET", object_key(digest, kind), "object identity mismatch")
                self.cache.reserve(size)
                extent = self.cache.install(ident, path, size, total, sha256)
            except BaseException:
                Path(path).unlink(missing_ok=True)
                raise
            self.count("fills")
            self._settle(ident, future, extent, None)
        except BaseException as exc:  # noqa: BLE001 - every joined reader sees it
            self._settle(ident, future, None, exc)

    def _wait(self, futures, deadline):
        for future in futures:
            future.result(timeout=max(0.001, deadline - time.monotonic()))

    def read(self, relative, first=None, last=None, suffix=None, *, background=False):
        """(object size, start, length, [(file, offset, count)]) for one range.

        ``first``/``last`` are inclusive like HTTP; ``suffix`` is ``bytes=-n``;
        neither means the whole object. Raises LookupError, RangeNotSatisfiable,
        TimeoutError or the fill's own error.
        """
        digest, kind = object_identity(relative)
        deadline, filled, began = time.monotonic() + FILL_DEADLINE_SECONDS + 5, False, time.monotonic()
        total = self.cache.total(digest, kind)
        if total is None:  # One fill tells the size.
            probe = 0 if first is None else first // self.extent_bytes
            self._wait([self.extent((digest, kind, probe), background)], deadline)
            filled, total = True, self.cache.total(digest, kind)
        if suffix is not None:
            first, last = max(0, total - suffix), total - 1
        elif first is None:
            first, last = 0, total - 1
        last = total - 1 if last is None else min(last, total - 1)
        if first >= total or last < first or last - first + 1 > MAX_RESPONSE_BYTES:
            raise RangeNotSatisfiable(total)
        try:
            for _attempt in range(3):
                pieces = self._open(digest, kind, first, last)
                missing = [index for index, piece in pieces if piece is None]
                if not missing:
                    break
                for _, piece in pieces:
                    if piece is not None:
                        piece[0].close()
                filled = True
                self._wait([self.extent((digest, kind, index), background) for index in missing], deadline)
            else:
                raise TimeoutError("chunk store extents were evicted while being read")
        except BaseException:
            self.count("errors")
            raise
        if filled:
            self.fill_wait.add(time.monotonic() - began)
        with self._guard:
            self.counters["requests"] += 1
            self.counters["misses" if filled else "hits"] += 1
            self.counters["bytes_served"] += last - first + 1
        output, size = [], self.extent_bytes
        for index, (stream, extent) in pieces:
            low, high = max(first, index * size), min(last + 1, index * size + extent.size)
            output.append((stream, low - index * size, high - low))
        return total, first, last - first + 1, output

    def _open(self, digest, kind, first, last):
        return [(index, self.cache.open((digest, kind, index)))
                for index in range(first // self.extent_bytes, last // self.extent_bytes + 1)]

    def missing(self, objects):
        """The keys of [(relative key, size or None)] not wholly in the cache.
        Without a size, the size the cache learned at its first fill."""
        result = []
        for relative, size in objects:
            digest, kind = object_identity(relative)
            total = self.cache.total(digest, kind) if size is None else size
            if total is None or not all(self.cache.contains((digest, kind, index))
                                        for index in range(-(-total // self.extent_bytes) or 1)):
                result.append(relative)
        return result

    def warm(self, items, concurrency=8):
        """Start a WarmJob over [(relative key, [(start, length)] or None)]."""
        parsed = []
        for relative, ranges in items:
            identity = object_identity(relative)
            if ranges is not None and any(type(start) is not int or type(length) is not int or start < 0
                                          or not 0 < length <= MAX_RESPONSE_BYTES for start, length in ranges):
                raise ValueError("warm ranges must be [start, length] pairs within 256 MiB")
            parsed.append((identity, ranges))
        with self._guard:
            job = WarmJob(next(self._ids), parsed, max(1, min(64, concurrency)))
            self._jobs[job.id] = job
            while len(self._jobs) > 64:
                self._jobs.popitem(last=False)
            self.counters["warm_jobs"] += 1
        threading.Thread(target=self._run_warm, args=(job,), name="chunk-store-warm", daemon=True).start()
        return job

    def job(self, identifier):
        with self._guard:
            return self._jobs.get(identifier)

    def _run_warm(self, job):
        work = queue.Queue()
        for (digest, kind), ranges in job.items:
            if ranges is None:
                work.put(("object", digest, kind, 0))
            else:
                for index in sorted({index for start, length in ranges for index in range(
                        start // self.extent_bytes, (start + length - 1) // self.extent_bytes + 1)}):
                    work.put(("extent", digest, kind, index))

        def one(task):
            what, digest, kind, index = task
            ident = (digest, kind, index)
            with job.guard:
                job.counts["extents"] += 1
            if self.cache.contains(ident):
                with job.guard:
                    job.counts["cached"] += 1
            else:
                with self._warm_slots:
                    extent = self.extent(ident, background=True).result(timeout=FILL_DEADLINE_SECONDS + 5)
                with job.guard:
                    job.counts["done"] += 1
                    job.counts["bytes"] += extent.size
            if what == "object":
                total = self.cache.total(digest, kind) or 0
                for later in range(1, -(-total // self.extent_bytes)):
                    work.put(("extent", digest, kind, later))

        def worker():
            while True:
                with job.guard:  # Only busy workers add tasks: empty and none busy is done.
                    try:
                        task = work.get_nowait()
                        busy[0] += 1
                    except queue.Empty:
                        if not busy[0]:
                            return
                        task = None
                if task is None:
                    time.sleep(.02)
                    continue
                try:
                    one(task)
                except Exception as exc:  # noqa: BLE001 - recorded in the job
                    with job.guard:
                        job.counts["failed"] += 1
                        job.errors.append(f"{task[1]}.{task[2]}.{task[3]}: {type(exc).__name__}: {exc}"[:240])
                finally:
                    with job.guard:
                        busy[0] -= 1

        busy = [0]
        threads = [threading.Thread(target=worker, daemon=True) for _ in range(job.concurrency)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        with job.guard:
            job.state = "failed" if job.counts["failed"] else "complete"
            job.seconds = time.monotonic() - job.started

    def metrics(self):
        with self._guard:
            counters, inflight = dict(self.counters), len(self._inflight)
            active = sum(job.state == "running" for job in self._jobs.values())
        with self.source._guard:
            s3 = dict(self.source.counters)
        return {**counters, "inflight_fills": inflight, "warm_jobs_active": active, "extent_bytes": self.extent_bytes,
                "fill_wait": self.fill_wait.summary(), "cache": self.cache.stats(),
                "serve": {name: samples.summary() for name, samples in self.serve.items()},
                "s3": {**s3, "ttfb": self.source.ttfb.summary(), "fill": self.source.latency.summary()},
                **({"mirror": dict(self.mirror.state)} if self.mirror is not None else {})}

    def close(self):
        for pool in (self._fills, self._background_fills):
            pool.shutdown(wait=False, cancel_futures=True)


# --- HTTP ---

class ReplicaMirror:
    """Keeps a replica node a copy of its S3 prefix (the permanent store):
    every ``interval`` it lists the prefix and fills what is not resident.
    A fresh or replaced node refills itself the same way."""

    def __init__(self, node, list_objects, prefix, *, interval=600, concurrency=16, sleep=None):
        self.node, self.list_objects, self.prefix = node, list_objects, prefix.strip("/") + "/"
        self.interval, self.concurrency = interval, concurrency
        self._stop = threading.Event()
        self._sleep = sleep or self._stop.wait
        self.state = {"rounds": 0, "last_at": None, "seconds": None, "listed": 0, "missing": 0, "filled": 0,
                      "failed": 0, "errors": []}

    def round(self):
        started = time.monotonic()
        objects = []
        for key, info in self.list_objects(self.prefix):
            relative = key[len(self.prefix):]
            try:
                object_identity(relative)
            except LookupError:
                continue  # Not a served kind (stored locators): the index reads those.
            objects.append((relative, info.size))
        missing = self.node.missing(objects)
        done = {"done": 0, "failed": 0, "errors": []}
        if missing:
            job = self.node.warm([(key, None) for key in missing], self.concurrency)
            while job.progress()["state"] == "running" and not self._stop.is_set():
                self._sleep(1.0)
            done = job.progress()
        self.state.update(rounds=self.state["rounds"] + 1, last_at=time.time(),
                          seconds=round(time.monotonic() - started, 1), listed=len(objects), missing=len(missing),
                          filled=done["done"], failed=done["failed"], errors=list(done["errors"])[:5])
        return dict(self.state)

    def run(self):
        while not self._stop.is_set():
            try:
                self.round()
            except Exception as exc:  # noqa: BLE001 - S3 listing outage: the next round retries
                _LOG.warning("chunk store replica round failed: %s: %s", type(exc).__name__, exc)
                self.state["errors"] = [f"{type(exc).__name__}: {exc}"[:200]]
            self._sleep(self.interval)

    def start(self):
        threading.Thread(target=self.run, name="chunk-store-mirror", daemon=True).start()
        return self

    def stop(self):
        self._stop.set()


class VirtualBlobs:
    """nydusd's blobs, rebuilt from packs (spike, docs/benchmarks/nydusd-spike-2026-10-03).

    A converter run with ``--nydusd-blobs`` keeps nydus's own chunk bytes and
    each blob's tail object: its chunk table, then the bytes after its last
    chunk. A blob is those chunks in order, located through the index (with
    the write token, as builders do), then the tail. Nothing here is trusted:
    nydusd checks every chunk against the TOC the signed bootstrap pins.
    """

    def __init__(self, node, index, *, cached=4096):
        self.node, self.index, self.cached = node, index, cached
        self._layouts, self._guard = OrderedDict(), threading.Lock()

    def _bytes(self, relative, first=None, last=None):
        """(object size, bytes) of one range."""
        total, _, length, pieces = self.node.read(relative, first, last)
        try:
            return total, b"".join(os.pread(stream.fileno(), count, offset) for stream, offset, count in pieces)
        finally:
            for stream, _, _ in pieces:
                stream.close()

    def layout(self, blob_id):
        """(size, segments): segments are (blob offset, length, object key,
        object offset), in order, covering the blob exactly."""
        with self._guard:
            if blob_id in self._layouts:
                self._layouts.move_to_end(blob_id)
                return self._layouts[blob_id]
        from .chunk_store import Locator, RAW, TAIL_ENTRY, TAIL_HEADER, ZSTD, decode_tail_table
        tail = f"meta/{blob_id}.tail"
        total, header = self._bytes(tail, 0, TAIL_HEADER.size - 1)
        count = TAIL_HEADER.unpack(header)[1]
        chunks, start = decode_tail_table(self._bytes(tail, 0, TAIL_HEADER.size + count * TAIL_ENTRY.size - 1)[1])
        try:  # Built at registration; the index is the fallback for older conversions.
            found = Locator.decode(self._bytes(object_key(blob_id, "layout"))[1])
        except NotFound:
            found = self.index.locate([digest for _, _, digest, _ in chunks])
        if len(found.entries) != len(chunks):
            raise ValueError("a blob layout does not match its tail table")
        segments = []
        for (coff, csize, _, compressed), (pack, offset, clen, flags) in zip(chunks, found.entries):
            if clen != csize or flags != (ZSTD if compressed else RAW):
                raise ValueError("the store holds this chunk re-encoded; convert with --nydusd-blobs")
            key, last = object_key(found.packs[pack][0], "pack"), segments[-1] if segments else None
            if last and last[2] == key and last[3] + last[1] == offset:
                segments[-1] = (last[0], last[1] + csize, key, last[3])
            else:
                segments.append((coff, csize, key, offset))
        end = chunks[-1][0] + chunks[-1][1]
        segments.append((end, total - start, tail, start))
        result = (end + total - start, tuple(segments))
        with self._guard:
            self._layouts[blob_id] = result
            while len(self._layouts) > self.cached:
                self._layouts.popitem(last=False)
        return result

    def read(self, component, blob_id, first=None, last=None, suffix=None, background=False):
        """Like ChunkStoreNode.read, over the rebuilt blob; ``component``
        (the request's repository) only names who asked."""
        size, segments = self.layout(blob_id)
        if suffix is not None:
            first, last = max(0, size - suffix), size - 1
        elif first is None:
            first, last = 0, size - 1
        last = size - 1 if last is None else min(last, size - 1)
        if first >= size or last < first or last - first + 1 > MAX_RESPONSE_BYTES:
            raise RangeNotSatisfiable(size)
        pieces = []
        try:
            for start, length, relative, offset in segments:
                low, high = max(first, start), min(last + 1, start + length)
                if low < high:
                    pieces += self.node.read(relative, offset + low - start, offset + high - start - 1,
                                             background=background)[3]
        except BaseException:
            for stream, _, _ in pieces:
                stream.close()
            raise
        return size, first, last - first + 1, pieces


def parse_range(header):
    """(first, last, suffix) of one ``bytes=`` range; all None for no header."""
    if not header:
        return None, None, None
    match = re.fullmatch(r"bytes=(\d{0,15})-(\d{0,15})", header.strip())
    if match is None or not any(match.groups()):
        raise ValueError("unsupported Range header")
    first, last = match.groups()
    if not first:
        if int(last) <= 0:
            raise ValueError("unsupported Range header")
        return None, None, int(last)
    return int(first), int(last) if last else None, None


class ChunkStoreServer:
    """HTTP/1.1 keep-alive on one asyncio loop (docs/benchmarks/chunk-store-node-2026-10-02).

    Every read is resolved, filled and faulted into the page cache in a thread
    pool; the loop only accepts and sends with sendfile(2). One GIL bounds it
    near one core: with ``native_server_sha256`` ucloud-chunk-serve answers
    resident reads in front of it, and this server sees misses and control.
    """

    def __init__(self, address, node, *, read_token, write_token, read_threads=512, blobs=None):
        read_token, write_token = (token.encode() if isinstance(token, str) else token
                                   for token in (read_token, write_token))
        if not write_token or not read_token or read_token == write_token:
            raise ValueError("the chunk store needs distinct read and write tokens")
        self.node, self.read_token, self.write_token = node, read_token, write_token
        self.blobs = blobs  # VirtualBlobs, for nydusd (spike); None serves none.
        self.socket = socket.create_server(address, reuse_port=False, backlog=4096)
        self.server_address = self.socket.getsockname()[:2]
        self._reads = ThreadPoolExecutor(max_workers=read_threads, thread_name_prefix="chunk-store-read")
        self._loop = self._stop = None
        self._ready, self._stopped = threading.Event(), threading.Event()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.server_close()

    def serve_forever(self):
        try:
            asyncio.run(self._serve())
        finally:
            self._stopped.set()

    async def _serve(self):
        self._loop, self._stop = asyncio.get_running_loop(), asyncio.Event()
        server = await asyncio.start_server(self._connection, sock=self.socket, limit=64 * 1024)
        self._ready.set()
        async with server:
            await self._stop.wait()

    def shutdown(self):
        """Stop a serve_forever running in another thread."""
        if self._ready.wait(10):
            self._loop.call_soon_threadsafe(self._stop.set)
            self._stopped.wait(10)

    def server_close(self):
        self.socket.close()
        self._reads.shutdown(wait=False, cancel_futures=True)

    async def _connection(self, reader, writer):
        writer.get_extra_info("socket").setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            while True:
                try:  # Idle keep-alive connections close after two minutes.
                    head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 120)
                except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError):
                    return
                try:
                    method, path, headers = _parse_head(head)
                    length = int(headers.get("content-length", "0"))
                    if not 0 <= length <= 16 * MIB:
                        raise ValueError("request body exceeds its bound")
                except ValueError:
                    return await _reply(writer, 400, {"error": "malformed request"})
                body = await reader.readexactly(length) if length else b""
                if not await self._dispatch(writer, method, path, headers, body):
                    return
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception:  # noqa: BLE001 - one connection's failure is not the server's
            _LOG.warning("chunk store connection failed", exc_info=True)
        finally:
            writer.close()

    def _authorized(self, headers, write):
        supplied = headers.get("authorization", "").removeprefix("Bearer ").encode()
        tokens = (self.write_token,) if write else (self.write_token, self.read_token)
        return any(hmac.compare_digest(supplied, token) for token in tokens)

    async def _dispatch(self, writer, method, path, headers, body):
        """Answer one request; False closes the connection."""
        node = self.node
        if method == "GET" and path == "/healthz":
            return await _reply(writer, 200, {"ok": True, **node.cache.stats()})
        if method == "GET" and path.startswith("/v1/objects/"):
            if not self._authorized(headers, write=False):
                return await _reply(writer, 401, {"error": "unauthorized"})
            return await self._object(writer, path.removeprefix("/v1/objects/"), headers.get("range"),
                                      background=self._authorized(headers, write=True))
        match = _VIRTUAL.fullmatch(path)
        if method in ("GET", "HEAD") and match and self.blobs is not None:
            if not self._authorized(headers, write=False):
                return await _reply(writer, 401, {"error": "unauthorized"})
            return await self._object(writer, match.groups(), headers.get("range"), head=method == "HEAD",
                                      background=self._authorized(headers, write=True))
        if not self._authorized(headers, write=not (method == "GET" and path == "/v1/metrics")):
            return await _reply(writer, 401, {"error": "unauthorized"})
        if method == "GET" and path == "/v1/metrics":
            return await _reply(writer, 200, node.metrics())
        match = re.fullmatch(r"/v1/warm/(\d{1,12})", path)
        if method == "GET" and match:
            job = node.job(int(match.group(1)))
            return await _reply(writer, 200, job.progress()) if job else await _reply(writer, 404, {"error": "no job"})
        if method == "POST" and path == "/v1/resident":
            try:
                keys = json.loads(body)["keys"]
                if not isinstance(keys, list) or len(keys) > 100_000 or not all(isinstance(k, str) for k in keys):
                    raise ValueError("resident takes up to 100,000 keys")
                missing = node.missing([(key, None) for key in keys])
            except (ValueError, KeyError, TypeError, LookupError) as exc:
                return await _reply(writer, 400, {"error": str(exc)[:200]})
            return await _reply(writer, 200, {"missing": missing})
        if method == "POST" and path == "/v1/warm":
            try:
                request = json.loads(body)
                items = [(item["key"], None if item.get("ranges") is None else [tuple(pair) for pair in item["ranges"]])
                         for item in request["objects"]]
                if not 0 < len(items) <= 100_000:
                    raise ValueError("warm takes 1 to 100,000 objects")
                job = node.warm(items, int(request.get("concurrency", 8)))
            except (ValueError, KeyError, TypeError, LookupError) as exc:
                return await _reply(writer, 400, {"error": str(exc)[:200]})
            return await _reply(writer, 202, job.progress())
        if method == "DELETE" and path.startswith("/v1/objects/"):  # Purge what an operator found bad.
            try:
                digest, kind = object_identity(path.removeprefix("/v1/objects/"))
            except LookupError:
                return await _reply(writer, 404, {"error": "not a chunk store object"})
            return await _reply(writer, 200, {"removed_extents": node.cache.remove(digest, kind)})
        return await _reply(writer, 404, {"error": "unknown endpoint"})

    async def _object(self, writer, relative, spec, head=False, background=False):
        """One object, or with ``relative`` = (component, blob id) one virtual blob.
        ``background``: a builder's read (the write token), filled from the
        background pool."""
        try:
            first, last, suffix = parse_range(spec)
            if isinstance(relative, tuple) and head:  # A blob's size: no bytes, whatever its size.
                size = (await self._loop.run_in_executor(self._reads, self.blobs.layout, relative[1]))[0]
                found = (size, 0, size, [])
            else:  # Off the loop: a layout needs the index, a fill S3, and the bytes the disk.
                read = (partial(self.blobs.read, *relative, first, last, suffix, background)
                        if isinstance(relative, tuple) else
                        partial(self.node.read, relative, first, last, suffix, background=background))
                found = await self._loop.run_in_executor(self._reads, _resident, read, self.node.serve,
                                                         time.monotonic())
        except NotFound:
            return await _reply(writer, 404, {"error": "no such object"})
        except LookupError:
            return await _reply(writer, 404, {"error": "not a chunk store object"})
        except RangeNotSatisfiable as exc:
            return await _reply(writer, 416, {"error": "range not satisfiable"},
                                [("Content-Range", f"bytes */{exc.total}")] if exc.total is not None else ())
        except ValueError as exc:
            if not isinstance(exc, RegistryRequestError):
                return await _reply(writer, 400, {"error": str(exc)[:200]})
            _LOG.warning("chunk store fill refused: %s", exc)
            return await _reply(writer, 502, {"error": "object store refused the fill"})
        except Exception as exc:  # noqa: BLE001 - S3 outage, timeout, disk: retryable for workers
            _LOG.warning("chunk store fill failed: %s: %s", type(exc).__name__, exc)
            return await _reply(writer, 503, {"error": "object store unavailable"})
        total, start, length, pieces = found
        sending = time.monotonic()
        try:
            lines = [f"HTTP/1.1 {206 if spec else 200} {'Partial Content' if spec else 'OK'}",
                     "Content-Type: application/octet-stream", f"Content-Length: {length}"]
            if spec:
                lines.append(f"Content-Range: bytes {start}-{start + length - 1}/{total}")
            writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())
            for stream, offset, count in () if head else pieces:  # Page cache to socket, no copy through Python.
                await self._loop.sendfile(writer.transport, stream, offset, count)
        finally:
            for stream, _, _ in pieces:
                stream.close()
        self.node.serve["send"].add(time.monotonic() - sending)
        return True


_FAULT = threading.local()


def _resident(read, serve, submitted):
    """``read()``, with its ranges faulted into the page cache, so the loop's
    sendfile never waits on the disk. On a Volume a cold read takes ~7 ms, and
    on the loop it stalled every request: hot 64 KiB reads went from 1.3 to 88 ms
    p50 behind 32 cold readers (2026-10-04). ``serve`` times the wait for a read
    thread and the read itself."""
    started = time.monotonic()
    serve["queue"].add(started - submitted)
    found = read()
    if not hasattr(_FAULT, "buffer"):
        _FAULT.buffer = memoryview(bytearray(1 << 20))
    for stream, offset, count in found[3]:
        end = offset + count
        while offset < end and (done := os.preadv(stream.fileno(), [_FAULT.buffer[:end - offset]], offset)) > 0:
            offset += done
    serve["read"].add(time.monotonic() - started)
    return found


def _parse_head(head):
    lines = head.decode("latin-1").split("\r\n")
    method, path, version = lines[0].split(" ")
    if not version.startswith("HTTP/1."):
        raise ValueError("unsupported HTTP version")
    headers = {}
    for line in lines[1:]:
        if line:
            name, separator, value = line.partition(":")
            if not separator:
                raise ValueError("malformed header")
            headers[name.strip().lower()] = value.strip()
    return method, path.split("?", 1)[0], headers


async def _reply(writer, status, payload, headers=()):
    """A JSON answer; errors close the connection (a body may be unread)."""
    payload = json.dumps(payload, sort_keys=True).encode()
    lines = [f"HTTP/1.1 {status} {HTTPStatus(status).phrase}", "Content-Type: application/json",
             f"Content-Length: {len(payload)}", *(f"{name}: {value}" for name, value in headers)]
    if status >= 400:
        lines.append("Connection: close")
    writer.write(("\r\n".join(lines) + "\r\n\r\n").encode() + payload)
    await writer.drain()
    return status < 400


class ChunkStoreClient:
    """Drivers (write token) warm the store and read its metrics."""

    def __init__(self, base_url, token, *, timeout=30.0, request=http_request):
        self.base_url, self.timeout, self._request = base_url.rstrip("/"), timeout, request
        self._headers = {"Authorization": "Bearer " + token}

    def _call(self, method, path, body=None):
        headers = dict(self._headers, **({"Content-Type": "application/json"} if body is not None else {}))
        payload = None if body is None else json.dumps(body, sort_keys=True).encode()
        return json.loads(self._request(method, self.base_url + path, headers=headers, body=payload,
                                        timeout=self.timeout, max_bytes=4 * MIB)[2])

    def warm(self, objects, *, concurrency=8):
        """``objects``: [{"key": relative key, "ranges": [[start, length], ...] or None}]."""
        return self._call("POST", "/v1/warm", {"objects": objects, "concurrency": concurrency})

    def missing(self, keys):
        """The keys not wholly resident on the node (never fills)."""
        return self._call("POST", "/v1/resident", {"keys": list(keys)})["missing"]

    def progress(self, job):
        return self._call("GET", f"/v1/warm/{int(job)}")

    def wait(self, job, *, timeout=3600.0, poll=1.0):
        deadline = time.monotonic() + timeout
        while True:
            progress = self.progress(job)
            if progress["state"] != "running" or time.monotonic() >= deadline:
                return progress
            time.sleep(poll)

    def metrics(self):
        return self._call("GET", "/v1/metrics")


def locator_objects(locator, base_url, token=None):
    """Warm items for one locator: its packs and metadata, as relative keys.

    With the node's ``token``, also every nydusd blob's tail and layout, which
    the node's virtual blobs read at each first attach: a cold one waits on
    S3's tail (M2 wave 1)."""
    prefix = base_url.rstrip("/") + "/v1/objects/"
    urls = [url for _, url in locator.packs] + list(locator.meta.values())
    if any(not url.startswith(prefix) for url in urls):
        raise ValueError("the locator does not name this chunk store node")
    keys = [url.removeprefix(prefix) for url in urls]
    if token is not None and "chunk_map" in locator.meta:
        from .chunk_index import http_range
        from .chunk_store import _MAP_HEADER, _MAP_REGION
        url, auth = locator.meta["chunk_map"], {"Authorization": "Bearer " + token}
        count = _MAP_HEADER.unpack(http_range(url, 0, _MAP_HEADER.size, headers=auth))[3]
        regions = http_range(url, 0, _MAP_HEADER.size + count * _MAP_REGION.size, headers=auth)[_MAP_HEADER.size:]
        for blob, _, _ in _MAP_REGION.iter_unpack(regions):
            keys += [object_key(blob.hex(), "tail"), object_key(blob.hex(), "layout")]
    return [{"key": key, "ranges": None} for key in dict.fromkeys(keys)]


# --- Commands: serve-chunk-store, warm-chunk-store ---

def build_node(store, environ=None):
    node_config = store.store_node
    source = S3Source(store.presigner(environ), store.prefix, concurrency=node_config.s3_concurrency)
    return ChunkStoreNode(ExtentCache(node_config.cache_dir, node_config.cache_bytes, node_config.replica), source,
                          extent_bytes=node_config.extent_bytes,
                          warm_concurrency=max(1, node_config.s3_concurrency // 4))


def serve_chunk_store(args):
    """The store node's ``ucloud-chunk-store``; exits 78 while not configured."""
    from .environment_config import ChunkStoreConfig, read_token
    store = ChunkStoreConfig.from_file(args.chunk_store_config)
    if store.store_node is None:
        print("immutable_environments.chunk_store.store_node is not configured", flush=True)
        return 78
    tokens = [read_token(path) for path in (store.read_token_file, store.write_token_file)]
    node = build_node(store)
    if store.store_node.replica:  # The permanent store's copy: mirror S3, never evict.
        node.mirror = ReplicaMirror(node, store.object_store().client.list_objects, store.prefix,
                                    interval=store.store_node.mirror_seconds).start()
    blobs = None
    if store.nydusd is not None:  # C2.1: workers' nydusd reads blobs rebuilt from packs.
        from .chunk_index import ChunkIndexClient
        blobs = VirtualBlobs(node, ChunkIndexClient(store.index_url, tokens[1].decode()))
    host, port = store.store_node.listen.rsplit(":", 1)
    if store.store_node.native_server_sha256:  # ucloud-chunk-serve holds the address; misses come here.
        host = "127.0.0.1"
    with ChunkStoreServer((host, int(port)), node, read_token=tokens[0], write_token=tokens[1],
                          blobs=blobs) as server:
        server.serve_forever()
    return 0


def warm_command(args):
    from .chunk_index import ChunkIndexClient
    from .chunk_convert import _chunk_store
    from .environment_config import read_token
    store = _chunk_store(args.config)
    if store is None or store.store_node is None:
        raise ValueError("warm-chunk-store needs immutable_environments.chunk_store.store_node")
    index = ChunkIndexClient(store.index_url, read_token(store.read_token_file).decode())
    token = read_token(store.write_token_file).decode()
    objects = [item for component in args.component
               for item in locator_objects(index.locator(component), store.store_node.url, token)]
    client = ChunkStoreClient(store.store_node.url, token)
    job = client.warm(list({item["key"]: item for item in objects}.values()), concurrency=args.concurrency)
    print(json.dumps(client.wait(job["job"], timeout=args.timeout) if args.wait else job, sort_keys=True))
    return 0


def add_commands(subparsers):
    serve = subparsers.add_parser("serve-chunk-store", help="Run a chunk store node (ucloud-chunk-store).")
    serve.add_argument("--chunk-store-config", type=Path, required=True,
                       help="JSON of immutable_environments.chunk_store, as node init writes it")
    serve.set_defaults(func=serve_chunk_store)
    warm = subparsers.add_parser("warm-chunk-store", help="Fill the store node with components' packs ahead of a burst.")
    warm.add_argument("--config", type=Path, required=True, help="deployment.json with chunk_store.store_node")
    warm.add_argument("--component", action="append", required=True, help="registered RAFS component digest")
    warm.add_argument("--concurrency", type=int, default=8)
    warm.add_argument("--wait", action="store_true")
    warm.add_argument("--timeout", type=float, default=3600.0)
    warm.set_defaults(func=warm_command)


# --- Store-node init: the "store" VM init role ---

STORE_CONFIG = "/etc/ucloud-sandboxes/chunk-store.json"
STORE_ENV = "/etc/ucloud-sandboxes/chunk-store.env"  # The S3 key; root-only, read by systemd.
STORE_DATA_MOUNT = "/mnt/store-replica"  # data_device; cache_dir and the index directory bind from it.
NATIVE_SERVER = "/usr/local/libexec/ucloud-sandboxes/ucloud-chunk-serve"  # From the bundle's runtime/chunk_serve.
_SAFE_SECRET = re.compile(r"[A-Za-z0-9+/=_.:-]{8,4096}")


def validate_store_options(options):
    """Store material goes to the store role only, and is well formed."""
    supplied = (options.chunk_store_config_json, options.chunk_store_read_token, options.chunk_store_write_token,
                options.chunk_store_s3_access_key_id, options.chunk_store_s3_secret_access_key)
    if options.role != "store":
        if any(supplied):
            raise ValueError("chunk store node material is only for the store role")
        return
    from .environment_config import ChunkStoreConfig
    store = ChunkStoreConfig.from_dict(json.loads(options.chunk_store_config_json or "null"))
    if store.store_node is None:
        raise ValueError("the store role needs immutable_environments.chunk_store.store_node")
    if (any(not _SAFE_SECRET.fullmatch(value) for value in supplied[1:])
            or not 32 <= min(len(supplied[1]), len(supplied[2]))
            or options.chunk_store_read_token == options.chunk_store_write_token):
        raise ValueError("the store role needs distinct index tokens and an S3 key")
    if options.environment_registry_url:
        raise ValueError("a store node takes no immutable-environment worker or builder material")


def _unit(description, exec_start, user, mounts=()):
    requires = f"RequiresMountsFor={' '.join(mounts)}\n" if mounts else ""
    return f"""[Unit]
Description={description}
Wants=network-online.target
After=network-online.target
{requires}
[Service]
Type=simple
User={user}
Group={user}
EnvironmentFile={STORE_ENV}
ExecStart={exec_start}
Restart=always
RestartSec=2
# Exit 78: not configured to run here; stay stopped.
SuccessExitStatus=78
RestartPreventExitStatus=78
LimitNOFILE=262144
TasksMax=infinity

[Install]
WantedBy=multi-user.target
"""


def store_init_script(options):
    """VM init for a store node: the bundle's agent runtime, the block, both
    index tokens, the S3 key and ``ucloud-chunk-store`` (plus
    ``ucloud-chunk-index`` with ``serve_index``). No Docker, no node agent,
    no heartbeats: placement never sees a store node."""
    import base64
    import shlex
    from .environment_config import ChunkStoreConfig
    store = ChunkStoreConfig.from_dict(json.loads(options.chunk_store_config_json))
    node, user, work = store.store_node, options.service_user, options.work_dir.rstrip("/")
    agent_bin, runtime = f"{work}/bin/ucloud-sandboxes", f"{work}/chunk-store-runtime"
    host, port = node.listen.rsplit(":", 1)
    probe = f"http://{'127.0.0.1' if host in ('0.0.0.0', '::', '[::]') else host}:{port}/healthz"
    b64 = lambda text: shlex.quote(base64.b64encode(text.encode()).decode())  # noqa: E731
    quote = shlex.quote
    secrets_file = (f"{store.access_key_id_env}={options.chunk_store_s3_access_key_id}\n"
                    f"{store.secret_access_key_env}={options.chunk_store_s3_secret_access_key}\n")
    binds = {"cache": node.cache_dir} | ({"index": str(Path(store.index_database).parent)} if node.serve_index else {})
    mounts = tuple(binds.values()) if node.data_device else ()
    command = lambda name: f"{agent_bin} {name} --chunk-store-config {STORE_CONFIG}"  # noqa: E731
    units = {"ucloud-chunk-store.service": _unit("UCloud chunk store node (chunk_store.store_node)",
                                                 command("serve-chunk-store"), user, mounts)}
    if node.serve_index:
        units["ucloud-chunk-index.service"] = _unit("UCloud chunk store index on the store node",
                                                    command("serve-chunk-index"), user, mounts)
    if node.native_server_sha256:
        units["ucloud-chunk-serve.service"] = _unit("UCloud chunk store reads (runtime/chunk_serve)",
                                                    f"{NATIVE_SERVER} --config {STORE_CONFIG}", user, mounts)
    lines = [f"printf %s {b64(text)} | base64 -d | $SUDO tee /etc/systemd/system/{name} >/dev/null"
             for name, text in units.items()]
    for name, wanted in (("ucloud-chunk-index", node.serve_index), ("ucloud-chunk-serve", node.native_server_sha256)):
        if not wanted:
            lines.append(f"$SUDO systemctl disable --now {name}.service >/dev/null 2>&1 || true")
    native = "" if not node.native_server_sha256 else "\n".join([  # Verified against the pin, then installed.
        '$SUDO tar --no-same-owner -xzf "$UCLOUD_PACKAGE_SPEC" -C "$UCLOUD_BUNDLE_TMP" '
        "runtime/chunk_serve/ucloud-chunk-serve",
        f'[ "$($SUDO sha256sum "$UCLOUD_BUNDLE_TMP/runtime/chunk_serve/ucloud-chunk-serve" | awk \'{{print $1}}\')" = '
        f'{quote(node.native_server_sha256)} ] || {{ echo "ucloud-chunk-serve does not match '
        f'store_node.native_server_sha256" >&2; exit 1; }}',
        f'$SUDO install -D -m 0755 -o root -g root "$UCLOUD_BUNDLE_TMP/runtime/chunk_serve/ucloud-chunk-serve" '
        f"{NATIVE_SERVER}"])
    directories = list(binds.values())
    data = ""
    if node.data_device:  # Never formatted here: a Volume arrives as ext4 and may already hold the replica.
        device, mount = quote(node.data_device), STORE_DATA_MOUNT
        data = "\n".join([
            f'[ "$($SUDO blkid -s TYPE -o value {device})" = ext4 ] '
            f'|| {{ echo "store_node.data_device is not ext4" >&2; exit 1; }}',
            f"$SUDO install -d -m 0755 {mount}",
            f"grep -q ' {mount} ' /etc/fstab || echo '{node.data_device} {mount} ext4 defaults,discard,nofail 0 2' "
            "| $SUDO tee -a /etc/fstab >/dev/null",
            f"mountpoint -q {mount} || $SUDO mount {mount}",
            f"$SUDO tune2fs -m 0 {device} >/dev/null",
            *(line for name, target in binds.items() for line in (
                f'$SUDO install -d -m 0700 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_USER" {mount}/{name} '
                f"{quote(target)}",
                f"grep -q ' {target} ' /etc/fstab || echo '{mount}/{name} {target} none "
                f"bind,nofail,x-systemd.requires-mounts-for={mount} 0 0' | $SUDO tee -a /etc/fstab >/dev/null",
                f"mountpoint -q {quote(target)} || $SUDO mount {quote(target)}"))])
    tokens = "\n".join(
        f'$SUDO install -d -m 0700 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_USER" "$(dirname {quote(path)})"\n'
        f'$SUDO install -m 0600 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_USER" /dev/null {quote(path)}\n'
        f"printf %s {b64(token)} | base64 -d | $SUDO tee {quote(path)} >/dev/null"
        for path, token in ((store.read_token_file, options.chunk_store_read_token),
                            (store.write_token_file, options.chunk_store_write_token)))
    return _STORE_SCRIPT.format(
        package=quote(options.package_spec), sha=quote(options.package_sha256), user=quote(user), work=quote(work),
        runtime=quote(runtime), agent=quote(agent_bin), node_id=quote(options.normalized_node_id()),
        config=b64(json.dumps(store.to_dict(), indent=2, sort_keys=True)), config_path=STORE_CONFIG,
        env=b64(secrets_file), env_path=STORE_ENV, data=data, native=native, tokens=tokens, units="\n".join(lines), names=" ".join(units),
        directories=" ".join(quote(path) for path in directories), probe=quote(probe), url=quote(node.url))


_STORE_SCRIPT = r"""#!/usr/bin/env bash
set -euo pipefail
if [ "$(id -u)" -eq 0 ]; then SUDO=""; else SUDO="sudo"; fi
export DEBIAN_FRONTEND=noninteractive
UCLOUD_PACKAGE_SPEC={package}
UCLOUD_PACKAGE_EXPECTED_SHA256={sha}
UCLOUD_SERVICE_USER={user}
UCLOUD_RUNTIME={runtime}
UCLOUD_AGENT_BIN={agent}
UCLOUD_INIT_STARTED_MS=$(( $(date +%s%N) / 1000000 ))
UCLOUD_INIT_PHASE_MS=$UCLOUD_INIT_STARTED_MS
log_init_phase() {{
  local now=$(( $(date +%s%N) / 1000000 ))
  echo "UCLOUD_INIT_PHASE name=$1 duration_ms=$((now - UCLOUD_INIT_PHASE_MS)) total_ms=$((now - UCLOUD_INIT_STARTED_MS))"
  UCLOUD_INIT_PHASE_MS=$now
}}
echo "Initializing UCloud chunk store node {node_id}"
id "$UCLOUD_SERVICE_USER" >/dev/null 2>&1 || $SUDO useradd --system --create-home --shell /usr/sbin/nologin "$UCLOUD_SERVICE_USER"
$SUDO install -d -m 0755 /etc/apt/apt.conf.d /etc/ucloud-sandboxes {work} {work}/bin
printf '%s\n' 'APT::Periodic::Enable "0";' 'APT::Periodic::Unattended-Upgrade "0";' \
  | $SUDO tee /etc/apt/apt.conf.d/99zz-ucloud-no-unattended-upgrades >/dev/null
$SUDO systemctl disable --now apt-daily.timer apt-daily-upgrade.timer >/dev/null 2>&1 || true
log_init_phase users
test -f "$UCLOUD_PACKAGE_SPEC" || {{ echo "A staged node package bundle is required" >&2; exit 1; }}
[ "$(sha256sum "$UCLOUD_PACKAGE_SPEC" | awk '{{print $1}}')" = "$UCLOUD_PACKAGE_EXPECTED_SHA256" ] \
  || {{ echo "Node package bundle checksum does not match the staged artifact" >&2; exit 1; }}
UCLOUD_BUNDLE_TMP="$($SUDO mktemp -d)"
trap '$SUDO rm -rf "$UCLOUD_BUNDLE_TMP"' EXIT
$SUDO tar --no-same-owner -xzf "$UCLOUD_PACKAGE_SPEC" -C "$UCLOUD_BUNDLE_TMP" package-bundle.json runtime/agent/node-agent-runtime.tar
$SUDO python3 - "$UCLOUD_BUNDLE_TMP" <<'PY'
import hashlib, json, sys
from pathlib import Path
root = Path(sys.argv[1])
agent = json.loads((root / "package-bundle.json").read_text())["runtime"]["agent"]
if agent.get("python") != f"{{sys.version_info.major}}.{{sys.version_info.minor}}":
    raise SystemExit("bundled agent runtime Python does not match this VM")
if hashlib.sha256((root / "runtime/agent/node-agent-runtime.tar").read_bytes()).hexdigest() != agent.get("sha256"):
    raise SystemExit("bundled agent runtime checksum mismatch")
PY
$SUDO rm -rf "$UCLOUD_RUNTIME.tmp"
$SUDO install -d -m 0755 "$UCLOUD_RUNTIME.tmp"
$SUDO tar --no-same-owner --no-same-permissions -xf "$UCLOUD_BUNDLE_TMP/runtime/agent/node-agent-runtime.tar" -C "$UCLOUD_RUNTIME.tmp"
test -d "$UCLOUD_RUNTIME.tmp/site-packages/ucloud_sandboxes"
$SUDO rm -rf "$UCLOUD_RUNTIME"
$SUDO mv "$UCLOUD_RUNTIME.tmp" "$UCLOUD_RUNTIME"
printf '#!/bin/sh\nexec env PYTHONPATH=%s/site-packages /usr/bin/python3 -m ucloud_sandboxes.cli "$@"\n' "$UCLOUD_RUNTIME" \
  | $SUDO tee "$UCLOUD_AGENT_BIN" >/dev/null
$SUDO chmod 0755 "$UCLOUD_AGENT_BIN"
{native}
log_init_phase runtime
printf %s {config} | base64 -d | $SUDO tee {config_path} >/dev/null
$SUDO chmod 0644 {config_path}
$SUDO install -m 0600 -o root -g root /dev/null {env_path}
printf %s {env} | base64 -d | $SUDO tee {env_path} >/dev/null
{data}
{tokens}
$SUDO install -d -m 0700 -o "$UCLOUD_SERVICE_USER" -g "$UCLOUD_SERVICE_USER" {directories}
# Bursts open hundreds of connections at once.
echo 'net.core.somaxconn = 4096' | $SUDO tee /etc/sysctl.d/90-ucloud-chunk-store.conf >/dev/null
$SUDO sysctl -q -p /etc/sysctl.d/90-ucloud-chunk-store.conf
log_init_phase config
{units}
$SUDO systemctl daemon-reload
for unit in {names}; do
  $SUDO systemctl enable "$unit" >/dev/null
  $SUDO systemctl restart "$unit"
done
for attempt in $(seq 1 120); do
  if python3 -c 'import sys, urllib.request; urllib.request.urlopen(sys.argv[1], timeout=2)' {probe} 2>/dev/null; then
    log_init_phase services
    echo "Chunk store node ready at {url}"
    exit 0
  fi
  sleep 0.5
done
echo "ucloud-chunk-store did not become healthy" >&2
$SUDO journalctl -u ucloud-chunk-store.service -n 50 --no-pager >&2 || true
exit 1
"""
