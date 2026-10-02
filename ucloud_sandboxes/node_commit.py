"""Worker half of C3.1 commit (docs/rl-state-primitives.md §3.2, §3.5, §8).

``POST /v1/sandboxes/{id}/commit-export`` is idempotent per operation ID and
doubles as the poll: 202 while exporting, 200 with the recorded result. Pause,
``runsc tar rootfs-upper`` and thaw run in the request under the lifecycle
lock, so no park, delete, migration or exec interleaves. The upload follows
unlocked: the staged tar is self-contained. Result files are a replay cache,
never authority; the worker parses and signs nothing.
"""
from __future__ import annotations

from dataclasses import replace
from http import HTTPStatus
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import threading
import time

from .commit_policy import CommitExport, CommitExportRequest, CommitPolicy, CommitRefused, staging_repository
from .direct_warden import DirectWardenError
from .sandbox import SandboxBusyError, SandboxConflictError, _atomic_write_json

_LOG = logging.getLogger(__name__)
SCRATCH_SLACK_BYTES = 64 * 1024**2
MIN_FREE_BYTES = 1024**3
# Gateways stop replaying once their row is staged; older records only cost inodes.
RESULT_RETENTION_SECONDS = 7 * 86400


class CommitExports:
    def __init__(self, service, *, registry=None) -> None:
        root = service.provisioner.registry.path.parent
        store = getattr(service.provisioner, "checkpoint_store", None)
        self.service = service
        # The push access snapshot publication already has.
        self.registry = registry if registry is not None else (store.registry if store else None)
        self.results, self.staging = root / "commit-exports", root / "commit-staging"
        for directory in (self.results, self.staging):
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        # No export outlives its process: a replay re-exports from the intent.
        # A killed tar may leave any entry type; none may stop the agent.
        for stale in self.staging.iterdir():
            (shutil.rmtree if stale.is_dir() and not stale.is_symlink() else os.unlink)(stale)
        horizon = time.time() - RESULT_RETENTION_SECONDS
        for record in self.results.iterdir():
            if record.lstat().st_mtime < horizon and not record.is_dir():
                record.unlink()
        self._guard = threading.Lock()
        self._active: dict[str, int] = {}  # operation -> reserved scratch bytes
        self._reserved = 0

    def request(self, sandbox_id: str, raw) -> tuple[int, dict]:
        request = CommitExportRequest.from_dict(raw)
        path = self.results / f"{request.operation_id}.json"
        with self._guard:
            recorded = self._load(path)
            if recorded is not None and (recorded.sandbox_id, recorded.request) != (sandbox_id, request):
                raise CommitRefused("commit_conflict", "operation_id is bound to another commit export")
            if request.operation_id in self._active or (recorded and recorded.state != "exporting"):
                return self._answer(recorded)
            if self.registry is None:
                raise CommitRefused("commit_export_unavailable", "this worker has no registry for commit staging",
                                    status=HTTPStatus.SERVICE_UNAVAILABLE)
            self._active[request.operation_id] = 0
        tar = self.staging / f"{request.operation_id}.tar"
        intent = None
        try:
            intent = self._export(sandbox_id, request, path, recorded, tar)
        finally:
            if intent is None or intent.state == "failed":
                tar.unlink(missing_ok=True)
                self._release(request.operation_id)
        if intent.state == "failed":
            return HTTPStatus.OK, {"export": intent.to_dict()}
        threading.Thread(target=self._upload, args=(intent, path, tar), daemon=True,
                         name=f"ucloud-commit-upload-{request.operation_id}").start()
        return HTTPStatus.ACCEPTED, {"export": intent.to_dict()}

    def _export(self, sandbox_id, request, path, recorded, tar) -> CommitExport:
        intents = []

        def prepare(spec, filestore, paused):
            # Before any pause: refusals here leave no record and no frozen sandbox.
            if filestore > request.max_bytes:
                raise CommitRefused("commit_too_large", "the sandbox's filesystem writes exceed max_bytes",
                                    status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
            self._reserve(request.operation_id, filestore + SCRATCH_SLACK_BYTES)
            # A crashed attempt's intent wins: it saw the state to restore.
            intents.append(recorded or CommitExport(
                sandbox_id, request, "exporting", paused,
                CommitPolicy.of(identity=self.service.provisioner.oci.platform_written_paths(spec)).identity,
                staging_repository(request.image_id)))
            if recorded is None:
                _atomic_write_json(path, intents[0].to_dict())
            return intents[0].was_paused

        try:
            self.service.export_upper(sandbox_id, generation=request.generation, destination=tar,
                                      resume=request.resume, prepare=prepare)
        except SandboxBusyError as exc:
            raise CommitRefused("commit_source_busy", str(exc), retryable=True) from exc
        except SandboxConflictError as exc:
            raise CommitRefused("commit_generation_mismatch", str(exc)) from exc
        except DirectWardenError as exc:
            if not intents:
                raise CommitRefused("commit_source_not_running", str(exc), retryable=True) from exc
            # The Warden restored the recorded state; the failure is final.
            _LOG.warning("commit export %s failed: %s", request.operation_id, exc)
            failed = replace(intents[0], state="failed", error_code="commit_export_failed")
            _atomic_write_json(path, failed.to_dict())
            return failed
        return intents[0]

    def _upload(self, intent: CommitExport, path: Path, tar: Path) -> None:
        operation = intent.request.operation_id
        try:
            try:
                digest, size = hashlib.sha256(), 0
                with tar.open("rb") as source:
                    while chunk := source.read(1 << 20):
                        digest.update(chunk)
                        size += len(chunk)
                blob = "sha256:" + digest.hexdigest()
                if not self.registry.blob_exists(intent.repository, blob):
                    self.registry.upload_blob_file(intent.repository, tar, blob, size)
            finally:
                tar.unlink(missing_ok=True)  # "staged" implies the scratch is gone
            _atomic_write_json(path, replace(intent, state="staged", blob_digest=blob, size=size).to_dict())
        except Exception:
            # The sandbox already holds its recorded state; the next replay re-exports.
            _LOG.exception("commit export %s could not be staged; a replay retries", operation)
            path.unlink(missing_ok=True)
        finally:
            # Only now may a replay start another export of this operation.
            self._release(operation)

    @staticmethod
    def _answer(recorded: CommitExport | None) -> tuple[int, dict]:
        if recorded is None:  # claimed by a request still pausing or exporting
            return HTTPStatus.ACCEPTED, {"export": None}
        status = HTTPStatus.ACCEPTED if recorded.state == "exporting" else HTTPStatus.OK
        return status, {"export": recorded.to_dict()}

    @staticmethod
    def _load(path: Path) -> CommitExport | None:
        try:
            return CommitExport.from_dict(json.loads(path.read_bytes()))
        except FileNotFoundError:
            return None

    def _reserve(self, operation: str, size: int) -> None:
        """Scratch for the tar: statvfs less every in-flight reservation (written
        bytes count twice until their upload ends, which errs toward refusing)."""
        space = os.statvfs(self.staging)
        with self._guard:
            if space.f_bavail * space.f_frsize - self._reserved - size < MIN_FREE_BYTES:
                raise CommitRefused("commit_capacity_unavailable", "no scratch space for the commit export",
                                    status=HTTPStatus.SERVICE_UNAVAILABLE, retryable=True)
            self._reserved += size
            self._active[operation] = size

    def _release(self, operation: str) -> None:
        with self._guard:
            self._reserved -= self._active.pop(operation, 0)
