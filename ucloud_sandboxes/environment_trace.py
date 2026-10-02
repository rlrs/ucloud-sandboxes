"""Node-local startup traces of immutable environment components (C2.3).

A trace is the ordered set of chunk indices first read during a bounded window
after a component's first attach on this node. It is untrusted hint data: a
replayed chunk is fetched through the verified cache like any demand miss,
and a missing, stale or corrupt trace only means demand loading.

``LocalTraceStore`` is the seam for cross-node distribution (plan C2.7): any
object with the same ``load``/``save`` contract, for example one reading a
signed ``trace`` artifact beside the component, can replace it.
"""
import json
import logging
import os
from pathlib import Path
import stat
import tempfile

from .environment_artifact import canonical_bytes, require_digest

_LOG = logging.getLogger(__name__)
TRACE_SCHEMA = "ucloud-environment-startup-trace-v1"
# Chunk-store images record chunk ids, which are global: one image's trace
# can warm another's chunks (docs/chunk-store-design.md §4 step 5).
TRACE_SCHEMA_IDS = "ucloud-environment-startup-trace-v2"
MAX_TRACE_CHUNKS = 2048
_MAX_TRACE_FILE_BYTES = 64 * 1024
# One trace per image ever attached grows without bound on a long-lived node
# (one image per task workloads); the oldest written are removed first.
MAX_TRACES = 4096


def trace_order(indices):
    """Contiguous runs of the trace, each scheduled at its first touch.

    Sorting into runs lets one bulk range fetch adjacent chunks; ordering runs
    by their earliest position keeps what startup needed first in front.
    """
    first = {}
    for position, index in enumerate(indices):
        first.setdefault(index, position)
    runs, current = [], []
    for index in sorted(first):
        if current and index != current[-1] + 1:
            runs.append(current)
            current = []
        current.append(index)
    if current:
        runs.append(current)
    runs.sort(key=lambda run: min(first[index] for index in run))
    return tuple(index for run in runs for index in run)


class LocalTraceStore:
    """At most ``max_traces`` small files, one per signed image digest, under a private directory."""

    def __init__(self, root: Path, *, max_traces=MAX_TRACES):
        self.root, self.max_traces = Path(root), max_traces
        if type(max_traces) is not int or max_traces < 1:
            raise ValueError("environment trace store needs a positive bound")
        if not self.root.is_absolute():
            raise ValueError("environment trace store must be absolute")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise ValueError("environment trace store must be a private owned directory")

    def _path(self, component):
        return self.root / (require_digest(component.image_digest)[7:] + ".json")

    def load(self, component):
        """("present", indices), ("absent", None) or ("invalid", None)."""
        path = self._path(component)
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return "absent", None
        except OSError:
            return "invalid", None
        try:
            with os.fdopen(descriptor, "rb") as source:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                    raise ValueError("environment trace is not an owned regular file")
                data = source.read(_MAX_TRACE_FILE_BYTES + 1)
            if len(data) > _MAX_TRACE_FILE_BYTES:
                raise ValueError("environment trace exceeds its bound")
            raw = json.loads(data)
            chunks = raw.get("chunks") if isinstance(raw, dict) else None
            if getattr(component, "chunk_ids", None) is not None:
                return "present", self._indices(component, raw, chunks)
            if (not isinstance(raw, dict) or set(raw) != {"schema", "image_digest", "chunk_count", "chunks"}
                    or raw["schema"] != TRACE_SCHEMA or raw["image_digest"] != component.image_digest
                    or raw["chunk_count"] != len(component.chunks) or not isinstance(chunks, list)
                    or not 0 < len(chunks) <= MAX_TRACE_CHUNKS or len(set(chunks)) != len(chunks)
                    or any(type(index) is not int or not 0 <= index < len(component.chunks) for index in chunks)):
                raise ValueError("invalid environment startup trace")
            return "present", tuple(chunks)
        except (OSError, ValueError, TypeError, AttributeError, RecursionError) as exc:
            # Disposable hint data: remove it so the next attach records anew.
            _LOG.warning("discarding environment startup trace %s: %s", path.name, exc)
            path.unlink(missing_ok=True)
            return "invalid", None

    @staticmethod
    def _indices(component, raw, chunks):
        if (not isinstance(raw, dict) or set(raw) != {"schema", "image_digest", "chunks"}
                or raw["schema"] != TRACE_SCHEMA_IDS or raw["image_digest"] != component.image_digest
                or not isinstance(chunks, list) or not 0 < len(chunks) <= MAX_TRACE_CHUNKS
                or len(set(chunks)) != len(chunks) or any(not isinstance(item, str) for item in chunks)):
            raise ValueError("invalid environment startup trace")
        indices = tuple(index for index in map(component.chunk_index, chunks) if index is not None)
        if not indices:
            raise ValueError("environment startup trace names none of this image's chunks")
        return indices

    def save(self, component, chunks):
        """Atomically replace this component's trace; no fsync, it is a hint."""
        chunks = list(dict.fromkeys(chunks))[:MAX_TRACE_CHUNKS]
        if not chunks:
            return
        ids = getattr(component, "chunk_ids", None)
        payload = canonical_bytes({"schema": TRACE_SCHEMA, "image_digest": component.image_digest,
                                   "chunk_count": len(component.chunks), "chunks": chunks} if ids is None else {
            "schema": TRACE_SCHEMA_IDS, "image_digest": component.image_digest,
            "chunks": list(dict.fromkeys(ids[index] for index in chunks))})
        descriptor, name = tempfile.mkstemp(prefix=".trace-", dir=self.root)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as target:
                target.write(payload)
            temporary.replace(self._path(component))
        finally:
            temporary.unlink(missing_ok=True)
        self._prune()

    def _prune(self):
        traces = []
        with os.scandir(self.root) as entries:
            for entry in entries:
                if entry.name.endswith(".json") and not entry.name.startswith("."):
                    try:
                        traces.append((entry.stat(follow_symlinks=False).st_mtime_ns, entry.name))
                    except FileNotFoundError:
                        pass
        traces.sort()
        for _, name in traces[:max(0, len(traces) - self.max_traces)]:
            (self.root / name).unlink(missing_ok=True)
