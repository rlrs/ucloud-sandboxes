"""Prepare local checkpoint replacements without owning the lifecycle fence.

Only adoption during a journaled mount or publication changes checkpoint
authority. Background work pins immutable inputs and may survive a wake or an
appended sealed layer.
"""
from __future__ import annotations

from dataclasses import replace
import fcntl
import json
import logging
import os
from pathlib import Path
import shutil
import tempfile
import threading
from typing import Any, Callable
from uuid import uuid4

from .storage_native_publication import local_layer_data_bytes, snapshot_compaction_start
from .storage_native_registry import consume_export_stream


LOGGER = logging.getLogger(__name__)


class _CompactionSuperseded(Exception):
    pass


class LocalCheckpointCompactor:
    def __init__(
        self, *, root: Path, global_config: Path, exporter: Any,
        load: Callable, remove_layers: Callable, max_layers: int = 8,
        max_delta_bytes: int = 4 * 1024**3, timeout_seconds: float = 120,
    ) -> None:
        self.root = root
        self.global_config = global_config
        self.exporter = exporter
        self.load = load
        self.remove_layers = remove_layers
        self.max_layers = max_layers
        self.max_delta_bytes = max_delta_bytes
        self.timeout_seconds = timeout_seconds
        self._guard = threading.RLock()
        self._pending: dict[str, Any] = {}
        self._thread: threading.Thread | None = None
        self._active = False
        self._completed = self._adopted = self._failed = self._deferred = 0
        self._input_bytes = self._output_bytes = 0

    def metrics(self) -> dict[str, int]:
        with self._guard:
            return {
                "local_compaction_active": int(self._active),
                "local_compaction_waiting": len(self._pending),
                "local_compaction_completed": self._completed,
                "local_compaction_adopted": self._adopted,
                "local_compaction_failed": self._failed,
                "local_compaction_deferred": self._deferred,
                "local_compaction_input_bytes": self._input_bytes,
                "local_compaction_output_bytes": self._output_bytes,
            }

    def submit(self, record: Any) -> None:
        # One background stream per node avoids multiplying I/O during a park
        # burst. Requests coalesce by volume; this never gates lifecycle work.
        if len(record.sealed_layer_paths) < 2:
            return
        if not self._guard.acquire(blocking=False):
            return  # another park/mount can retry; never wait for maintenance
        try:
            self._pending[record.volume_id] = record
            if self._thread is None:
                self._thread = threading.Thread(target=self._run, name="local-checkpoint-compaction", daemon=True)
                try:
                    self._thread.start()
                except Exception:
                    self._thread = None
                    self._pending.pop(record.volume_id, None)
                    LOGGER.exception("could not start local compaction")
        finally:
            self._guard.release()

    def wait(self, timeout: float | None = None) -> None:
        """Wait for current background work during qualification/shutdown."""
        with self._guard:
            thread = self._thread
        if thread is not None:
            thread.join(timeout)

    def _run(self) -> None:
        while True:
            with self._guard:
                if not self._pending:
                    self._active = False
                    self._thread = None
                    return
                volume_id = next(iter(self._pending))
                record = self._pending.pop(volume_id)
                self._active = True
            try:
                current = self.load(volume_id)
                if (self._matches(current, record.owner.request_fields(), record.sealed_layer_paths)
                        and current.state.value != "publishing"):
                    self._compact(current)
            except _CompactionSuperseded:
                with self._guard:
                    self._deferred += 1
            except Exception:
                with self._guard:
                    self._failed += 1
                LOGGER.exception("local checkpoint compaction failed for %s", volume_id)

    @staticmethod
    def _matches(record: Any, owner: dict, paths: tuple[str, ...]) -> bool:
        return bool(
            record is not None and record.owner.request_fields() == owner
            and record.state.value not in {"deleted", "deleting", "error"}
            and tuple(record.sealed_layer_paths[:len(paths)]) == tuple(paths)
        )

    def _manifest_path(self, volume_id: str) -> Path:
        return self.root / volume_id / "local-compaction.json"

    def _read(self, volume_id: str) -> dict | None:
        try:
            payload = json.loads(self._manifest_path(volume_id).read_text())
            output = Path(payload["output"])
            if (payload["version"] != 1 or output.parent != self.root / volume_id
                    or not output.name.startswith("local-compact-") or output.suffix != ".commit"
                    or not isinstance(payload["sources"], list) or not payload["sources"]
                    or type(payload["start"]) is not int
                    or not 0 <= payload["start"] < len(payload["sources"])
                    or type(payload["size"]) is not int or payload["size"] <= 0):
                raise ValueError("invalid local compaction candidate")
            return payload
        except FileNotFoundError:
            return None

    def cleanup_abandoned(self, record: Any) -> None:
        """Reclaim crash-left input pins without disturbing a live export."""
        root = self.root / record.volume_id
        for work in root.glob(".local-compact-*"):
            if not work.is_dir() or work.is_symlink():
                continue
            try:
                with (work / ".lock").open("a") as lock:
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        continue
                    current = self.load(record.volume_id)
                    if current is None:
                        continue
                    try:
                        output = root / (work / "output-name").read_text()
                        ready = self._read(record.volume_id)
                        if (output.parent == root and output.name.startswith("local-compact-")
                                and output.suffix == ".commit"
                                and str(output) not in (*current.sealed_layer_paths, *current.cached_layer_paths)
                                and (ready is None or ready["output"] != str(output))):
                            output.unlink(missing_ok=True)
                    except (OSError, ValueError, TypeError, KeyError):
                        pass  # retain uncertain output; input pins are never authority
                    shutil.rmtree(work)
            except OSError:
                LOGGER.warning("local compaction scratch cleanup deferred", exc_info=True)

    def adopt(self, record: Any, persist: Callable) -> Any:
        """Adopt only inside a journaled mount or publication transition."""
        if record.state.value not in {"acquiring", "publishing"}:
            return record
        if not self._guard.acquire(blocking=False):
            return record
        try:
            return self._adopt_locked(record, persist)
        finally:
            self._guard.release()

    def _adopt_locked(self, record: Any, persist: Callable) -> Any:
        with self._guard:
            try:
                candidate = self._read(record.volume_id)
                if candidate is None or not self._matches(record, candidate["owner"], candidate["sources"]):
                    return record
                output = Path(candidate["output"])
                if output.stat().st_size != candidate["size"]:
                    return record
            except (OSError, ValueError, KeyError, TypeError):
                # A damaged optimization must not hide the original checkpoint.
                return record
            paths = record.sealed_layer_paths
            end, start = len(candidate["sources"]), candidate["start"]
            obsolete = paths[start:end]
            updated = replace(
                record, sealed_layer_paths=(*paths[:start], str(output), *paths[end:]),
                cached_layer_paths=tuple(dict.fromkeys((*record.cached_layer_paths, *obsolete))),
            )
            persist(updated)  # durable before any input name is removed
            self._adopted += 1
            # If cleanup is interrupted, the next attempt recognizes output as
            # journal-owned and cannot discard it as an unused candidate.
            try:
                self._manifest_path(record.volume_id).unlink(missing_ok=True)
                self.remove_layers(tuple(Path(path) for path in obsolete))
            except OSError:
                LOGGER.warning("local compaction cleanup deferred for %s", record.volume_id, exc_info=True)
            return updated

    def _compact(self, record: Any) -> None:
        paths = tuple(record.sealed_layer_paths)
        if len(paths) < 2:
            return
        manifest = self._manifest_path(record.volume_id)
        with self._guard:
            try:
                ready = self._read(record.volume_id)
            except (ValueError, KeyError, TypeError):
                # Never trust a damaged manifest's output path for deletion.
                manifest.unlink(missing_ok=True)
                ready = None
            if ready is not None:
                try:
                    intact = Path(ready["output"]).stat().st_size == ready["size"]
                except FileNotFoundError:
                    intact = False
                if self._matches(record, ready["owner"], ready["sources"]) and intact:
                    return  # reusable at the next mount, even after more seals
                if ready["output"] not in (*record.sealed_layer_paths, *record.cached_layer_paths):
                    self.remove_layers((Path(ready["output"]),))
                manifest.unlink(missing_ok=True)
        sizes = tuple(local_layer_data_bytes(Path(path)) for path in paths)
        start = snapshot_compaction_start(
            sizes, max_layers=self.max_layers, max_delta_bytes=self.max_delta_bytes,
            reusable_base=not record.published_layers,
        )
        if start is None:
            return
        estimate = sum(sizes[start:])
        # Background maintenance may not consume the last free filesystem
        # space. Bound actual output too; estimates of sparse data are not quotas.
        reserve = 256 * 1024**2
        budget = min(estimate * 2 + 64 * 1024**2, shutil.disk_usage(manifest.parent).free - reserve)
        if budget < estimate:
            with self._guard:
                self._deferred += 1
            return
        output = manifest.parent / f"local-compact-{uuid4().hex}.commit"
        committed = False
        lock = None
        try:
            with tempfile.TemporaryDirectory(prefix=".local-compact-", dir=manifest.parent) as raw:
                work = Path(raw)
                # Startup reconciliation can remove crash-left hardlinks, but
                # must never remove the inputs of an export still in flight.
                lock = (work / ".lock").open("x")
                fcntl.flock(lock, fcntl.LOCK_EX)
                (work / "output-name").write_text(output.name)
                inputs = []
                for index, path in enumerate(paths[start:]):
                    pinned = work / f"input-{index}.commit"
                    os.link(path, pinned)
                    inputs.append({"file": str(pinned)})
                source = work / "source.json"
                source.write_text(json.dumps({"lowers": inputs, "upper": {}, "resultFile": "", "repoBlobUrl": ""}))

                def check_current():
                    current = self.load(record.volume_id)
                    if (not self._matches(current, record.owner.request_fields(), paths)
                            or current.state.value == "publishing"):
                        raise _CompactionSuperseded("local compaction inputs superseded")

                with output.open("xb") as stream:
                    written = 0

                    def consume(chunk):
                        nonlocal written
                        if (written + len(chunk) > budget
                                or shutil.disk_usage(manifest.parent).free - len(chunk) < reserve):
                            raise OSError("local compaction output exceeds available maintenance space")
                        stream.write(chunk)
                        written += len(chunk)

                    descriptor = consume_export_stream(
                        lambda sock: self.exporter.export_compacted_image(
                            source_image_config=source, global_config=self.global_config,
                            stream_socket_path=sock,
                        ),
                        # Short socket names also permit local macOS qualification.
                        stream_socket_root=self.root, chunk_bytes=1024**2,
                        timeout_seconds=self.timeout_seconds, consume=consume, check_current=check_current,
                    )
                    stream.flush()
                    os.fsync(stream.fileno())
                payload = {"version": 1, "owner": record.owner.request_fields(), "sources": paths,
                           "start": start, "output": str(output), "size": descriptor.size,
                           "digest": descriptor.digest}
                staged = work / "ready.json"
                with staged.open("x") as stream:
                    json.dump(payload, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
                with self._guard:
                    check_current()
                    os.replace(staged, manifest)
                    committed = True  # never delete a now-visible candidate
                    directory = os.open(manifest.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    except OSError:
                        manifest.unlink(missing_ok=True)
                        raise
                    finally:
                        os.close(directory)
                    self._completed += 1
                    self._input_bytes += estimate
                    self._output_bytes += descriptor.size
        finally:
            if lock is not None:
                lock.close()
            if not committed:
                output.unlink(missing_ok=True)
