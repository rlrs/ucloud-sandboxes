"""Bounded disposable cache of independently authenticated environment chunks."""
from collections import OrderedDict
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor, TimeoutError as FutureTimeout
from dataclasses import dataclass, field
from http.client import IncompleteRead
import logging
import math
import os
from pathlib import Path
import stat
from ssl import SSLCertVerificationError
import tempfile
from threading import Condition, Event, Lock, Thread
import time
from urllib.error import URLError

from .environment_artifact import CHUNK_BYTES, content_digest
from .managed_registry import RegistryRequestError

_LOG = logging.getLogger(__name__)
# One bulk prefetch request covers at most this many adjacent chunks (4 MiB).
PREFETCH_RANGE_CHUNKS = 16
_PREFETCH_KINDS = ("metadata", "trace")  # Scheduling priority, first wins.


@dataclass
class _ChunkFetch:
    cancel: Event
    future: Future[bytes] | None = None
    readers: int = 0


class PrefetchJob:
    """A bounded, best-effort fill of verified chunks for one component.

    It never answers a reader with an error: a reader that joined a failed
    prefetch fetches the chunk itself. ``cancel`` (detach, close) stops
    scheduling and installs nothing more; budgets and the deadline only stop
    scheduling, and already started bulk reads complete normally.
    """

    def __init__(self, component, kind, indices, *, max_bytes, max_chunks, deadline_seconds):
        if kind not in _PREFETCH_KINDS:
            raise ValueError("unknown environment prefetch kind")
        self.component, self.kind = component, kind
        self.queue = list(dict.fromkeys(index for index in indices
                                        if type(index) is int and 0 <= index < len(component.chunks)))
        self.queue.reverse()  # pop() from the end yields the requested order.
        self.max_bytes, self.max_chunks = max_bytes, max_chunks
        self.started = time.monotonic()
        self.deadline = self.started + deadline_seconds
        self.cancelled, self.done = Event(), Event()
        self.inflight = self.scheduled_bytes = self.scheduled_chunks = 0
        self.fetched_chunks = self.fetched_bytes = self.failed_chunks = self.skipped_chunks = 0
        self.outcome = None  # complete, budget, deadline or cancelled
        self.seconds = None

    def cancel(self):
        self.cancelled.set()

    def wait(self, timeout=None):
        return self.done.wait(timeout)

    def stop(self, outcome):
        # Cancellation wins over budget or deadline: in-flight bytes are dropped.
        if not self.done.is_set() and (self.outcome is None or outcome == "cancelled"):
            self.outcome = outcome
        self.queue.clear()


@dataclass
class _Recording:
    component: object
    sink: object
    deadline: float
    max_chunks: int
    chunks: dict = field(default_factory=dict)


class VerifiedEnvironmentCache:
    def __init__(self, root: Path, registry, *, max_bytes=1024 ** 3, concurrent_misses=8,
                 fetch_timeout_seconds=30.0, prefetch_slots=None):
        if not root.is_absolute() or max_bytes < CHUNK_BYTES or concurrent_misses < 1:
            raise ValueError("invalid environment cache bounds")
        if not math.isfinite(fetch_timeout_seconds) or fetch_timeout_seconds <= 0:
            raise ValueError("environment fetch timeout must be positive and finite")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise ValueError("environment cache must be a private owned directory")
        self.root, self.registry, self.max_bytes = root, registry, max_bytes
        self._guard = Condition()
        self._concurrent_misses = concurrent_misses
        # Prefetch shares the miss pool but never takes more than these slots,
        # and never starts while a demand miss waits for one.
        self._prefetch_slots = max(1, concurrent_misses // 4) if prefetch_slots is None else prefetch_slots
        if not 0 < self._prefetch_slots <= concurrent_misses:
            raise ValueError("invalid environment prefetch slots")
        self._fetch_timeout_seconds = fetch_timeout_seconds
        self._executor = ThreadPoolExecutor(max_workers=concurrent_misses, thread_name_prefix="environment-fetch")
        self._closed = False
        self._pending = {}
        self._prefetching = {}  # Chunk digest -> Future of an in-flight bulk read.
        self._prefetch_ops = self._demand_waiting = 0
        self._jobs = []
        # Bytes scheduled by unfinished jobs. Trace replays stop once all jobs
        # together reach half the cache, so concurrent attaches cannot evict
        # each other's fills. Metadata, which attach waits for, counts toward
        # this but is bounded only by its own budget: a replay holding the
        # share must never starve a later attach of its metadata.
        self._prefetch_scheduled = 0
        self._recordings = {}  # Signed image digest -> _Recording.
        self._trace_guard = Lock()
        self._background = None
        self._lru = OrderedDict()
        self._bytes = 0
        self._metrics = {"hits": 0, "misses": 0, "downloaded_bytes": 0, "corruptions": 0, "fetch_retries": 0,
                         "prefetch_joined_reads": 0, "traces_recorded": 0, "trace_chunks_recorded": 0}
        for kind in _PREFETCH_KINDS:
            for name in ("jobs", "chunks", "bytes", "failed_chunks", "skipped_chunks", "truncated", "seconds"):
                self._metrics[f"{kind}_prefetch_{name}"] = 0.0 if name == "seconds" else 0
        # Cache files are disposable, not authority. Rebuild bounded LRU metadata;
        # actual bytes are authenticated on every use, including after restart.
        for path in sorted(root.iterdir(), key=lambda p: p.name):
            if len(path.name) != 64 or any(c not in "0123456789abcdef" for c in path.name):
                continue
            info = path.lstat()
            if stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid():
                self._lru[path.name] = info.st_size
                self._bytes += info.st_size
        self._evict()

    @staticmethod
    def _cancelled(cancel):
        if cancel is not None and cancel.is_set():
            raise CancelledError("environment read cancelled")

    def _evict(self):
        while self._bytes > self.max_bytes and self._lru:
            name, size = self._lru.popitem(last=False)
            self._bytes -= size
            try:
                (self.root / name).unlink()
            except FileNotFoundError:
                pass

    def _cached(self, chunk):
        path = self.root / chunk.digest.removeprefix("sha256:")
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return None
        with os.fdopen(descriptor, "rb") as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size != chunk.size:
                data = b""
            else:
                data = source.read(chunk.size + 1)
        if len(data) != chunk.size or content_digest(data) != chunk.digest:
            with self._guard:
                self._metrics["corruptions"] += 1
                self._bytes -= self._lru.pop(path.name, 0)
                path.unlink(missing_ok=True)
            return None
        with self._guard:
            self._metrics["hits"] += 1
            if path.name in self._lru:
                self._lru.move_to_end(path.name)
        return data

    def chunk(self, chunk, *, cancel=None, source=None):
        """Return a verified chunk; ``source`` is (image digest, offset) for range reads."""
        self._cancelled(cancel)
        with self._guard:
            if self._closed:
                raise CancelledError("environment cache is closed")
        data = self._cached(chunk)
        if data is not None:
            return data
        join_prefetch, deadline = True, None
        while True:
            prefetched = None
            with self._guard:
                while True:
                    self._cancelled(cancel)
                    if self._closed:
                        raise CancelledError("environment cache is closed")
                    pending = self._pending.get(chunk.digest)
                    if pending is not None and not pending.cancel.is_set():
                        pending.readers += 1
                        break
                    prefetched = self._prefetching.get(chunk.digest) if join_prefetch else None
                    if prefetched is not None:
                        self._metrics["prefetch_joined_reads"] += 1
                        break
                    if pending is None and len(self._pending) + self._prefetch_ops < self._concurrent_misses:
                        pending = _ChunkFetch(Event(), readers=1)
                        pending.future = self._executor.submit(self._fetch, chunk, pending.cancel, source, deadline)
                        self._pending[chunk.digest] = pending
                        pending.future.add_done_callback(lambda done: self._finished(chunk.digest, done))
                        break
                    self._demand_waiting += 1
                    try:
                        self._guard.wait(.05)
                    finally:
                        self._demand_waiting -= 1
            if prefetched is None:
                break
            # A shared bulk read already carries this chunk. Its failure is
            # not this reader's: fall back to an ordinary demand miss. Joining
            # and falling back share one fetch budget, which the kernel's NBD
            # request timeout matches; a stalled single-attempt range must not
            # double a reader's latency past it and kill the export.
            deadline = time.monotonic() + self._fetch_timeout_seconds
            data = self._await(prefetched, cancel, self._fetch_timeout_seconds / 2)
            if data is not None and len(data) == chunk.size:
                return data
            join_prefetch = False
        # Cancelling one sandbox's read must not cancel a shared miss needed by
        # another sandbox. At most concurrent_misses HTTP operations survive,
        # each uses the fetch retry budget and existing registry socket timeout.
        assert pending.future is not None
        try:
            while True:
                self._cancelled(cancel)
                try:
                    data = pending.future.result(timeout=.05)
                    if len(data) != chunk.size:
                        raise ValueError("shared chunk has inconsistent size")
                    return data
                except FutureTimeout:
                    if pending.future.done():
                        raise
        finally:
            with self._guard:
                pending.readers -= 1
                if pending.readers == 0:
                    # A cancelled sole reader cannot keep retrying useless work.
                    # A sibling reader retains the same shared fetch obligation.
                    pending.cancel.set()

    def _await(self, future, cancel, timeout):
        """A joined bulk read's bytes, or None once it fails or ``timeout`` passes."""
        limit = time.monotonic() + timeout
        while True:
            self._cancelled(cancel)
            remaining = limit - time.monotonic()
            if remaining <= 0:
                return None
            try:
                return future.result(timeout=min(.05, remaining))
            except FutureTimeout:
                continue
            except (CancelledError, Exception):
                return None

    def _finished(self, digest, pending):
        with self._guard:
            entry = self._pending.get(digest)
            if entry is not None and entry.future is pending:
                self._pending.pop(digest)
            self._guard.notify_all()

    def _install(self, chunk, data, cancel, deadline=None):
        """Atomically add verified bytes; cancellation or expiry installs nothing."""
        descriptor, name = tempfile.mkstemp(prefix=".chunk-", dir=self.root)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as target:
                target.write(data)
            temporary.chmod(0o400)
            destination = self.root / chunk.digest.removeprefix("sha256:")
            with self._guard:
                self._cancelled(cancel)
                if self._closed:
                    raise CancelledError("environment cache is closed")
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError("immutable environment fetch deadline exceeded")
                temporary.replace(destination)
                self._bytes -= self._lru.pop(destination.name, 0)
                self._lru[destination.name] = len(data)
                self._bytes += len(data)
                self._metrics["downloaded_bytes"] += len(data)
                self._evict()
        finally:
            temporary.unlink(missing_ok=True)

    def _fetch(self, chunk, cancel, source=None, deadline=None):
        if deadline is None:
            deadline = time.monotonic() + self._fetch_timeout_seconds
        backoff = .05
        while True:
            self._cancelled(cancel)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("immutable environment fetch deadline exceeded")
            try:
                if source is not None:
                    image_digest, offset = source
                    data = self.registry.client.blob_range(
                        self.registry.repository, image_digest, offset, chunk.size,
                        timeout_seconds=remaining)
                else:
                    data = self.registry.client.blob_bytes(
                        self.registry.repository, chunk.digest, max_bytes=chunk.size,
                        timeout_seconds=remaining)
                break
            except RegistryRequestError as exc:
                if exc.status_code not in {408, 429, 500, 502, 503, 504}:
                    raise
            except SSLCertVerificationError:
                raise
            except URLError as exc:
                if isinstance(exc.reason, SSLCertVerificationError):
                    raise
            except (OSError, IncompleteRead):
                pass
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("immutable environment fetch deadline exceeded")
            if cancel.wait(min(backoff, remaining)):
                raise CancelledError("environment fetch cancelled")
            self._cancelled(cancel)
            if time.monotonic() >= deadline:
                raise TimeoutError("immutable environment fetch deadline exceeded")
            with self._guard:
                self._metrics["fetch_retries"] += 1
            backoff = min(1.0, backoff * 2)
        self._cancelled(cancel)
        if time.monotonic() >= deadline:
            raise TimeoutError("immutable environment fetch deadline exceeded")
        if len(data) != chunk.size or content_digest(data) != chunk.digest:
            raise ValueError("environment chunk content identity mismatch")
        self._install(chunk, data, cancel, deadline)
        with self._guard:
            self._metrics["misses"] += 1
        return data

    def prefetch(self, component, indices, *, kind, max_bytes, max_chunks=65536, deadline_seconds=30.0):
        """Schedule verified bulk fills of ``indices`` in order; returns its job.

        Chunks already cached or in flight are skipped. Indices are untrusted:
        out-of-range entries are dropped, the rest verify like any miss.
        """
        job = PrefetchJob(component, kind, indices, max_bytes=max_bytes, max_chunks=max_chunks,
                          deadline_seconds=deadline_seconds)
        with self._guard:
            self._metrics[f"{kind}_prefetch_jobs"] += 1
            if self._closed:
                job.stop("cancelled")
                self._complete(job)
                return job
            self._jobs.append(job)
            self._jobs.sort(key=lambda item: _PREFETCH_KINDS.index(item.kind))
            self._start_background()
            self._guard.notify_all()
        return job

    def record_startup(self, component, sink, *, window_seconds=30.0, max_chunks=2048):
        """Record chunk indices first read in a bounded window after attach.

        ``sink(component, indices)`` receives the ordered set once the window
        ends by time or count. Returns False when already recording.
        """
        with self._guard:
            if self._closed:
                return False
            with self._trace_guard:
                if component.image_digest in self._recordings:
                    return False
                self._recordings[component.image_digest] = _Recording(
                    component, sink, time.monotonic() + window_seconds, max_chunks)
            self._start_background()
            self._guard.notify_all()
        return True

    def stop_recording(self, component):
        """Discard an unfinished window, for example when the component detaches."""
        with self._trace_guard:
            recording = self._recordings.get(component.image_digest)
            if recording is not None and recording.component == component:
                del self._recordings[component.image_digest]

    def cancel_prefetch(self, component):
        with self._guard:
            for job in self._jobs:
                if job.component == component:
                    job.cancel()
            self._guard.notify_all()

    def _observe(self, component, index):
        with self._trace_guard:
            recording = self._recordings.get(component.image_digest)
            if recording is None or index in recording.chunks or len(recording.chunks) >= recording.max_chunks:
                return
            recording.chunks[index] = None
            full = len(recording.chunks) >= recording.max_chunks
        if full:
            with self._guard:
                self._guard.notify_all()

    def _start_background(self):
        if self._background is None:
            self._background = Thread(target=self._background_loop, name="environment-prefetch", daemon=True)
            self._background.start()

    def _prefetch_slot(self):
        return (not self._demand_waiting and self._prefetch_ops < self._prefetch_slots
                and len(self._pending) + self._prefetch_ops < self._concurrent_misses)

    def _complete(self, job):
        self._prefetch_scheduled -= job.scheduled_bytes
        job.seconds = time.monotonic() - job.started
        prefix = f"{job.kind}_prefetch_"
        self._metrics[prefix + "seconds"] += job.seconds
        if job.outcome in ("budget", "deadline"):
            self._metrics[prefix + "truncated"] += 1
        job.done.set()

    def _next_run(self, job):
        """Take the next adjacent missing chunks of ``job`` within its budget."""
        whole = getattr(self.registry, "whole_image", None)
        limit = PREFETCH_RANGE_CHUNKS if whole is not None and whole(job.component) else 1
        run, seen = [], set()
        while job.queue and len(run) < limit:
            index = job.queue[-1]
            chunk = job.component.chunks[index]
            name = chunk.digest.removeprefix("sha256:")
            busy = (name in self._lru or chunk.digest in self._pending
                    or chunk.digest in self._prefetching or chunk.digest in seen)
            if run and (busy or index != run[-1][0] + 1):
                break
            job.queue.pop()
            if busy:
                job.skipped_chunks += 1
                self._metrics[f"{job.kind}_prefetch_skipped_chunks"] += 1
                continue
            if (job.scheduled_chunks >= job.max_chunks or job.scheduled_bytes + chunk.size > job.max_bytes
                    or (job.kind != "metadata" and self._prefetch_scheduled + chunk.size > self.max_bytes // 2)):
                job.stop("budget")
                break
            job.scheduled_chunks += 1
            job.scheduled_bytes += chunk.size
            self._prefetch_scheduled += chunk.size
            seen.add(chunk.digest)
            run.append((index, chunk))
        return run

    def _background_loop(self):
        with self._guard:
            while not self._closed:
                now = time.monotonic()
                dispatched = False
                for job in list(self._jobs):
                    if job.cancelled.is_set():
                        job.stop("cancelled")
                    elif now >= job.deadline:
                        job.stop("deadline")
                    elif not job.queue and not job.inflight:
                        job.stop("complete")
                    if job.outcome is None and self._prefetch_slot():
                        run = self._next_run(job)
                        if run:
                            self._dispatch(job, run)
                            dispatched = True
                            break
                    if job.outcome is not None and not job.inflight:
                        self._jobs.remove(job)
                        self._complete(job)
                finished = self._expired_recordings(now)
                if finished:
                    self._guard.release()
                    try:
                        self._persist(finished)
                    finally:
                        self._guard.acquire()
                    continue
                if not dispatched:
                    deadlines = [job.deadline for job in self._jobs]
                    with self._trace_guard:
                        deadlines += [recording.deadline for recording in self._recordings.values()]
                    # Slot releases, new work and close all notify; the short
                    # bound only covers a demand waiter leaving without one.
                    self._guard.wait(min([.25, *(max(0.0, value - now) for value in deadlines)]) + .001
                                     if deadlines else None)

    def _expired_recordings(self, now):
        with self._trace_guard:
            finished = [digest for digest, recording in self._recordings.items()
                        if now >= recording.deadline or len(recording.chunks) >= recording.max_chunks]
            return [self._recordings.pop(digest) for digest in finished]

    def _persist(self, recordings):
        for recording in recordings:
            if not recording.chunks:
                continue  # Nothing read: the next attach records again.
            try:
                recording.sink(recording.component, tuple(recording.chunks))
            except Exception:
                _LOG.warning("could not persist an environment startup trace", exc_info=True)
                continue
            with self._guard:
                self._metrics["traces_recorded"] += 1
                self._metrics["trace_chunks_recorded"] += len(recording.chunks)

    def _dispatch(self, job, run):
        futures = {}
        for _index, chunk in run:
            futures[chunk.digest] = self._prefetching[chunk.digest] = Future()
        self._prefetch_ops += 1
        job.inflight += 1
        try:
            self._executor.submit(self._fetch_run, job, run, futures)
        except BaseException:
            self._prefetch_ops -= 1
            job.inflight -= 1
            for digest, future in futures.items():
                self._prefetching.pop(digest, None)
                future.set_exception(CancelledError("environment prefetch was not scheduled"))
            job.stop("cancelled")

    def _fetch_run(self, job, run, futures):
        """One bulk read without retries: a failure only leaves demand misses."""
        verified, installed = {}, []
        try:
            self._cancelled(job.cancelled)
            first, total = run[0][0], sum(chunk.size for _, chunk in run)
            client, repository = self.registry.client, self.registry.repository
            if len(run) == 1 and not getattr(self.registry, "whole_image", lambda _: False)(job.component):
                pieces = [client.blob_bytes(repository, run[0][1].digest, max_bytes=run[0][1].size,
                                            timeout_seconds=self._fetch_timeout_seconds)]
            else:
                payload = client.blob_range(repository, job.component.image_digest, first * CHUNK_BYTES, total,
                                            timeout_seconds=self._fetch_timeout_seconds)
                if len(payload) != total:
                    raise ValueError("environment prefetch range has an unexpected length")
                pieces, offset = [], 0
                for _, chunk in run:
                    pieces.append(payload[offset:offset + chunk.size])
                    offset += chunk.size
            for (_index, chunk), data in zip(run, pieces):
                if len(data) == chunk.size and content_digest(data) == chunk.digest:
                    verified[chunk.digest] = data
            for _index, chunk in run:
                if chunk.digest in verified:
                    try:
                        self._install(chunk, verified[chunk.digest], job.cancelled)
                        installed.append(chunk)
                    except (CancelledError, OSError):
                        pass
        except Exception as exc:
            _LOG.info("environment %s prefetch range failed: %s", job.kind, exc)
        finally:
            with self._guard:
                for digest in futures:
                    self._prefetching.pop(digest, None)
                self._prefetch_ops -= 1
                job.inflight -= 1
                prefix = f"{job.kind}_prefetch_"
                job.fetched_chunks += len(installed)
                job.fetched_bytes += sum(chunk.size for chunk in installed)
                job.failed_chunks += len(run) - len(verified)
                self._metrics[prefix + "chunks"] += len(installed)
                self._metrics[prefix + "bytes"] += sum(chunk.size for chunk in installed)
                self._metrics[prefix + "failed_chunks"] += len(run) - len(verified)
                self._guard.notify_all()
            # Verified bytes may answer joined readers even if not installed.
            for digest, future in futures.items():
                if digest in verified:
                    future.set_result(verified[digest])
                else:
                    future.set_exception(ValueError("environment prefetch did not verify this chunk"))

    def close(self):
        with self._guard:
            self._closed = True
            for pending in self._pending.values():
                pending.cancel.set()
            for job in self._jobs:
                job.cancel()
            with self._trace_guard:
                self._recordings.clear()
            self._guard.notify_all()
        if self._background is not None:
            self._background.join(5)
        self._executor.shutdown(wait=True, cancel_futures=True)
        with self._guard:
            for job in self._jobs:
                job.stop("cancelled")
                self._complete(job)
            self._jobs.clear()

    def read(self, component, offset, length, *, cancel=None):
        if (type(offset) is not int or type(length) is not int or offset < 0
                or length < 0 or length > 32 * 1024 ** 2 or offset + length > component.image_size):
            raise ValueError("environment read exceeds its authenticated bounds")
        result = []
        end = offset + length
        while offset < end:
            index, within = divmod(offset, CHUNK_BYTES)
            if self._recordings:
                self._observe(component, index)
            whole = getattr(self.registry, "whole_image", None)
            source = (component.image_digest, index * CHUNK_BYTES) if whole and whole(component) else None
            data = self.chunk(component.chunks[index], cancel=cancel, source=source)
            take = min(len(data) - within, end - offset)
            result.append(data[within:within + take])
            offset += take
        self._cancelled(cancel)
        return b"".join(result)

    def metrics(self):
        with self._guard:
            return self._metrics | {"cached_bytes": self._bytes, "pending_misses": len(self._pending),
                                    "prefetch_jobs_active": len(self._jobs),
                                    "prefetch_ranges_inflight": self._prefetch_ops}
