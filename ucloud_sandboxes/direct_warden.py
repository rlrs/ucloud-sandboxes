from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import select
import signal
import stat
import subprocess
import tempfile
import time
from typing import Iterator, Protocol, Sequence

from .hibernation import (
    HibernationArtifactStore,
    HibernationAuthority,
    HibernationFileRole,
    HibernationJournal,
    HibernationJournalStore,
    HibernationManifest,
    HibernationReconciler,
    HibernationRecord,
    HibernationRecoveryAction,
    HibernationRuntimeFingerprint,
    HibernationState,
    LocalHibernationArtifactFile,
    hibernation_process_identity_matches,
    linux_process_start_time_ticks,
)
from .storage_native_daemon import (
    StorageNativeNodeClient,
    StorageVolumeOwner,
    StorageVolumeRecord,
    StorageVolumeState,
)
from .telemetry import Telemetry


_SAFE_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_CONTAINER_ID = re.compile(r"[0-9a-f]{64}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_LOG = logging.getLogger(__name__)
_APPLICATION_MEMORY = "application_memory.img"
_ACTIVE_APPLICATION_MEMORY = "application_memory.active"
_CHECKPOINT_STATE = "checkpoint.img"
_PAGES_METADATA = "pages_meta.img"
_PRIVATE_PAGES = "pages.img"


class DirectWardenError(RuntimeError):
    pass


@dataclass(frozen=True)
class CommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str = ""
    stderr: str = ""


class CommandRunner(Protocol):
    def run(
        self,
        argv: Sequence[str],
        *,
        timeout: float,
    ) -> CommandResult: ...


class SubprocessCommandRunner:
    def run(
        self,
        argv: Sequence[str],
        *,
        timeout: float,
    ) -> CommandResult:
        command = tuple(str(item) for item in argv)
        # runsc create/restore daemonize children that can retain inherited
        # descriptors. Pipes would keep communicate() waiting for EOF after the
        # runsc parent has exited. Seekable files preserve diagnostics without
        # tying command completion to the sentry/gofer descriptor lifetime.
        with (
            tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stdout,
            tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stderr,
        ):
            result = subprocess.run(
                command,
                text=True,
                stdout=stdout,
                stderr=stderr,
                timeout=timeout,
                check=False,
            )
            stdout.seek(0)
            stderr.seek(0)
            rendered_stdout = stdout.read()
            rendered_stderr = stderr.read()
        return CommandResult(
            argv=command,
            returncode=result.returncode,
            stdout=rendered_stdout,
            stderr=rendered_stderr,
        )


class ProcessHandle(Protocol):
    pid: int
    start_time_ticks: int

    def alive(self) -> bool: ...

    def terminate(self, *, timeout: float) -> None: ...

    def close(self) -> None: ...


class ProcessFencer(Protocol):
    def open(self, pid: int, start_time_ticks: int) -> ProcessHandle: ...


class RootfsMountLifecycle(Protocol):
    def park_sandbox(self, sandbox: "DirectSandbox") -> None: ...

    def resume_sandbox(self, sandbox: "DirectSandbox") -> None: ...


class LinuxPidfdHandle:
    """An exact process reference held across capture and publication."""

    def __init__(
        self,
        pid: int,
        start_time_ticks: int,
        pidfd: int,
        *,
        proc_root: Path,
    ) -> None:
        self.pid = pid
        self.start_time_ticks = start_time_ticks
        self.pidfd = pidfd
        self.proc_root = proc_root
        self._closed = False

    def alive(self) -> bool:
        if self._closed:
            return False
        poller = select.poll()
        poller.register(self.pidfd, select.POLLIN)
        if poller.poll(0):
            return False
        return hibernation_process_identity_matches(
            self.pid,
            self.start_time_ticks,
            proc_root=self.proc_root,
        )

    def terminate(self, *, timeout: float) -> None:
        if self._closed:
            raise DirectWardenError("process fence is already closed")
        pidfd_send_signal = getattr(signal, "pidfd_send_signal", None)
        if pidfd_send_signal is None:
            raise DirectWardenError(
                "pidfd_send_signal is required for exact sentry fencing"
            )
        if self.alive():
            pidfd_send_signal(self.pidfd, signal.SIGKILL, None, 0)
        poller = select.poll()
        poller.register(self.pidfd, select.POLLIN)
        if not poller.poll(max(1, int(timeout * 1000))):
            raise DirectWardenError(
                f"timed out waiting for sentry PID {self.pid} to exit"
            )
        # A non-child can remain visible as a zombie until its parent reaps it.
        # pidfd readability is the kernel's authoritative exited condition; the
        # exact pidfd, not a numeric PID lookup, received SIGKILL.

    def close(self) -> None:
        if not self._closed:
            os.close(self.pidfd)
            self._closed = True


class LinuxPidfdFencer:
    def __init__(self, *, proc_root: Path = Path("/proc")) -> None:
        self.proc_root = proc_root

    def open(self, pid: int, start_time_ticks: int) -> LinuxPidfdHandle:
        pidfd_open = getattr(os, "pidfd_open", None)
        if pidfd_open is None:
            raise DirectWardenError("pidfd_open is required for exact sentry fencing")
        if not hibernation_process_identity_matches(
            pid,
            start_time_ticks,
            proc_root=self.proc_root,
        ):
            raise DirectWardenError("sentry identity changed before fencing")
        try:
            descriptor = pidfd_open(pid, 0)
        except OSError as exc:
            raise DirectWardenError("could not open sentry pidfd") from exc
        if not hibernation_process_identity_matches(
            pid,
            start_time_ticks,
            proc_root=self.proc_root,
        ):
            os.close(descriptor)
            raise DirectWardenError("sentry identity changed while fencing")
        return LinuxPidfdHandle(
            pid,
            start_time_ticks,
            descriptor,
            proc_root=self.proc_root,
        )


@dataclass(frozen=True)
class DirectRunscWardenConfig:
    runsc: Path
    runtime_root: Path
    memory_root: Path
    bundle_root: Path
    journal_root: Path
    runtime_fingerprint: HibernationRuntimeFingerprint
    proc_root: Path = Path("/proc")
    network: str = "none"
    command_timeout_seconds: float = 60.0
    stop_timeout_seconds: float = 30.0
    readiness_command: tuple[str, ...] = ("/bin/true",)

    def __post_init__(self) -> None:
        for label, path in (
            ("runsc", self.runsc),
            ("runtime_root", self.runtime_root),
            ("memory_root", self.memory_root),
            ("bundle_root", self.bundle_root),
            ("journal_root", self.journal_root),
            ("proc_root", self.proc_root),
        ):
            if not path.is_absolute():
                raise ValueError(f"{label} must be absolute")
        if self.command_timeout_seconds <= 0 or self.stop_timeout_seconds <= 0:
            raise ValueError("Warden timeouts must be positive")
        if not self.readiness_command:
            raise ValueError("readiness_command cannot be empty")


@dataclass(frozen=True)
class DirectSandbox:
    sandbox_id: str
    sandbox_generation: int
    container_id: str
    spec_sha256: str
    rootfs_sha256: str
    bundle: Path
    memory_directory: str

    def __post_init__(self) -> None:
        if not _SAFE_COMPONENT.fullmatch(self.sandbox_id):
            raise ValueError("sandbox_id is invalid")
        if self.sandbox_generation < 0:
            raise ValueError("sandbox_generation must be non-negative")
        if not _CONTAINER_ID.fullmatch(self.container_id):
            raise ValueError("container_id must be a full lowercase SHA-256")
        if not _DIGEST.fullmatch(self.spec_sha256):
            raise ValueError("spec_sha256 must be a lowercase SHA-256")
        if not _DIGEST.fullmatch(self.rootfs_sha256):
            raise ValueError("rootfs_sha256 must be a lowercase SHA-256")
        if not self.bundle.is_absolute():
            raise ValueError("bundle must be absolute")
        if not _SAFE_COMPONENT.fullmatch(self.memory_directory):
            raise ValueError("memory_directory is invalid")


class DirectRunscWarden:
    """Single node owner for direct-runsc sandbox task lifecycles.

    Docker/containerd may materialize the bundle's immutable rootfs, but this
    owner alone invokes runsc create, checkpoint, restore, exec, and delete.
    """

    def __init__(
        self,
        config: DirectRunscWardenConfig,
        *,
        runner: CommandRunner | None = None,
        fencer: ProcessFencer | None = None,
        storage: StorageNativeNodeClient,
        rootfs_lifecycle: RootfsMountLifecycle,
        telemetry: Telemetry | None = None,
    ) -> None:
        self.config = config
        self.runner = runner or SubprocessCommandRunner()
        self.fencer = fencer or LinuxPidfdFencer(proc_root=config.proc_root)
        self.storage = storage
        self.rootfs_lifecycle = rootfs_lifecycle
        self.telemetry = telemetry or Telemetry.disabled("direct-runsc-warden")
        self.journals = HibernationJournalStore(config.journal_root)
        self.artifacts = HibernationArtifactStore(
            config.memory_root,
            preserve_incarnation_roots=True,
            require_stable_device=False,
        )
        self._ensure_roots()

    def create(self, sandbox: DirectSandbox, *, operation_id: str) -> HibernationRecord:
        with self._locked(sandbox):
            self._validate_bundle(sandbox)
            active_memory = self._active_memory_root(sandbox)
            active_memory.mkdir(mode=0o700, parents=True, exist_ok=True)
            self._require_private_directory(active_memory, "active memory directory")
            self._checked(
                *self._common(),
                "create",
                f"--bundle={sandbox.bundle}",
                sandbox.container_id,
            )
            try:
                self._checked(
                    *self._state_prefix(),
                    "start",
                    sandbox.container_id,
                )
                pid, ticks = self._state_identity(sandbox)
                return self._journal(sandbox).initialize_running(
                    sandbox_id=sandbox.sandbox_id,
                    sandbox_generation=sandbox.sandbox_generation,
                    spec_sha256=sandbox.spec_sha256,
                    operation_id=operation_id,
                    sentry_pid=pid,
                    sentry_start_time_ticks=ticks,
                )
            except Exception:
                self._best_effort_delete(sandbox)
                raise

    def _readiness_command(self, sandbox: DirectSandbox) -> tuple[str, ...]:
        try:
            config = json.loads((sandbox.bundle / "config.json").read_text())
        except FileNotFoundError:
            return self.config.readiness_command
        if (
            config.get("annotations", {}).get("dev.ucloud-sandboxes.file-helper")
            == "v1"
        ):
            return ("/.ucloud-job-init", "files", "ready")
        return self.config.readiness_command

    def exec(
        self,
        sandbox: DirectSandbox,
        argv: Sequence[str],
    ) -> CommandResult:
        if not argv:
            raise ValueError("exec argv cannot be empty")
        with self._locked(sandbox):
            record = self._require_state(sandbox, HibernationState.RUNNING)
            if record.authority != HibernationAuthority.LIVE:
                raise DirectWardenError("running sandbox has no live authority")
            return self._checked(
                *self._state_prefix(),
                "exec",
                sandbox.container_id,
                *argv,
            )

    @contextmanager
    def exec_lease(
        self,
        sandbox: DirectSandbox,
        argv: Sequence[str],
        *,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
        user: str | None = None,
    ) -> Iterator[tuple[str, ...]]:
        """Hold the cross-process lifecycle fence for a streaming runsc exec."""
        if not argv or any(not isinstance(item, str) or "\0" in item for item in argv):
            raise ValueError("exec argv must be a non-empty NUL-free string list")
        if working_dir is not None and (
            not working_dir.startswith("/") or "\0" in working_dir
        ):
            raise ValueError("exec working directory must be absolute")
        if user is not None and (
            not re.fullmatch(r"[0-9]+(?::[0-9]+)?", user) or "\0" in user
        ):
            raise ValueError("direct exec user must be numeric uid or uid:gid")
        environment = env or {}
        for key, value in environment.items():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) or "\0" in value:
                raise ValueError("direct exec environment is invalid")
        with self._locked(sandbox):
            record = self._require_state(sandbox, HibernationState.RUNNING)
            if record.authority != HibernationAuthority.LIVE:
                raise DirectWardenError("running sandbox has no live authority")
            command = [*self._state_prefix(), "exec"]
            if working_dir is not None:
                command.append(f"--cwd={working_dir}")
            if user is not None:
                command.append(f"--user={user}")
            for key, value in sorted(environment.items()):
                command.append(f"--env={key}={value}")
            command.extend((sandbox.container_id, *argv))
            yield tuple(command)

    def inspect(self, sandbox: DirectSandbox) -> HibernationRecord | None:
        """Read one incarnation's durable lifecycle state under its fence."""
        with self._locked(sandbox):
            return self._journal(sandbox).load()

    def inspect_snapshot(self, sandbox: DirectSandbox) -> HibernationRecord | None:
        """Read durable lifecycle state without joining an active operation.

        Heartbeats and inventory reads must not wait for a streaming exec, park,
        restore, or migration to release the per-sandbox lifecycle fence.  The
        journal itself is atomically replaced, so this returns a complete, if
        possibly immediately superseded, revision suitable for observation.
        """

        return self._journal(sandbox).load_snapshot()

    def load_parked_manifest(self, sandbox: DirectSandbox) -> HibernationManifest:
        """Load portable checkpoint metadata without mounting parked storage."""
        with self._locked(sandbox):
            parked = self._require_state(sandbox, HibernationState.PARKED)
            path = self._parked_manifest_path(sandbox)
            try:
                info = path.lstat()
                if (
                    not path.is_file()
                    or path.is_symlink()
                    or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077
                ):
                    raise DirectWardenError(
                        "parked manifest control copy is not private"
                    )
                payload = path.read_bytes()
                if len(payload) > 1024 * 1024:
                    raise DirectWardenError("parked manifest control copy is too large")
                manifest = HibernationManifest.from_dict(
                    json.loads(payload.decode("ascii"))
                )
            except DirectWardenError:
                raise
            except (
                FileNotFoundError,
                OSError,
                UnicodeDecodeError,
                json.JSONDecodeError,
                TypeError,
                ValueError,
            ) as exc:
                raise DirectWardenError(
                    "parked manifest control copy is unavailable"
                ) from exc
            manifest.validate_identity(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                spec_sha256=sandbox.spec_sha256,
                runtime_sha256=self._runtime_fingerprint(sandbox).digest,
            )
            self._require_managed_process_ledger(sandbox, manifest)
            if (
                manifest.hibernation_generation != parked.hibernation_generation
                or manifest.metadata_sha256 != parked.manifest_sha256
            ):
                raise DirectWardenError("parked manifest control copy changed identity")
            return manifest

    def publish_storage_snapshot(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> StorageVolumeRecord:
        with self._locked(sandbox):
            lifecycle = self._require_state(sandbox, HibernationState.PARKED)
            if lifecycle.state != HibernationState.PARKED:
                raise DirectWardenError(
                    "only a parked sandbox can publish storage authority"
                )
            record = self._storage_record(sandbox)
            if record.state == StorageVolumeState.PUBLISHED:
                return record
            if record.state == StorageVolumeState.MOUNTED:
                self.rootfs_lifecycle.park_sandbox(sandbox)
            record = self.storage.ensure_published(
                self._storage_owner(sandbox),
                operation_id=operation_id,
            )
            if record.state != StorageVolumeState.PUBLISHED:
                raise DirectWardenError(
                    "storage-native publication returned an invalid record"
                )
            return record

    def running_process_alive(self, sandbox: DirectSandbox) -> bool:
        """Prove that a RUNNING journal still owns the recorded sentry."""
        with self._locked(sandbox):
            record = self._journal(sandbox).load()
            return bool(
                record is not None
                and record.state == HibernationState.RUNNING
                and hibernation_process_identity_matches(
                    record.sentry_pid,
                    record.sentry_start_time_ticks,
                    proc_root=self.config.proc_root,
                )
            )

    def adopt_parked(
        self,
        sandbox: DirectSandbox,
        manifest: HibernationManifest,
    ) -> HibernationRecord:
        """Adopt a destination-local migration artifact without starting runsc."""
        with self._locked(sandbox):
            manifest.validate_identity(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                spec_sha256=sandbox.spec_sha256,
                runtime_sha256=self._runtime_fingerprint(sandbox).digest,
            )
            published = self.artifacts.load_complete(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                hibernation_generation=manifest.hibernation_generation,
            )
            if published.metadata_sha256 != manifest.metadata_sha256:
                raise DirectWardenError(
                    "migrated generation changed before Warden adoption"
                )
            journal = self._journal(sandbox)
            if journal.load() is None:
                # A deterministic container ID from an interrupted earlier
                # import must not survive underneath newly adopted authority.
                self._best_effort_delete(sandbox)
            parked = journal.initialize_parked(published)
            self._persist_parked_manifest(sandbox, published)
            return parked

    def park(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> HibernationRecord:
        with self._locked(sandbox):
            journal = self._journal(sandbox)
            running = self._require_state(sandbox, HibernationState.RUNNING)
            if running.sentry_pid is None or running.sentry_start_time_ticks is None:
                raise DirectWardenError("running journal lacks a sentry identity")
            handle = self.fencer.open(
                running.sentry_pid,
                running.sentry_start_time_ticks,
            )
            try:
                with self.telemetry.span("sandbox.park.prepare"):
                    hibernating = journal.begin_hibernate(
                        operation_id=operation_id,
                        expected_revision=running.revision,
                    )
                    generation = self.artifacts.prepare_generation(
                        sandbox_id=sandbox.sandbox_id,
                        sandbox_generation=sandbox.sandbox_generation,
                        hibernation_generation=hibernating.hibernation_generation,
                    )
                try:
                    with self.telemetry.span("sandbox.park.runsc_checkpoint"):
                        self._checked(
                            *self._common(),
                            "checkpoint",
                            "--hibernate",
                            f"--image-path={generation}",
                            sandbox.container_id,
                        )
                    if not handle.alive():
                        raise DirectWardenError(
                            "sentry exited before its capture was durably published"
                        )
                    with self.telemetry.span("sandbox.park.commit_artifact"):
                        manifest = self._manifest(sandbox, hibernating, generation)
                        manifest = self.artifacts.publish_complete(manifest)
                        self._persist_parked_manifest(sandbox, manifest)
                except Exception:
                    if os.path.lexists(generation / self.artifacts.COMPLETE_NAME):
                        raise
                    self._rollback_capture(
                        sandbox,
                        journal=journal,
                        hibernating=hibernating,
                        generation=generation,
                        handle=handle,
                    )
                    raise

                # COMPLETE is now authoritative. Never resume this backend.
                with self.telemetry.span("sandbox.park.stop_runtime"):
                    handle.terminate(timeout=self.config.stop_timeout_seconds)
                    pending = journal.mark_sentry_reaped(
                        operation_id=operation_id,
                        expected_revision=hibernating.revision,
                    )
                    self._checked(
                        *self._state_prefix(),
                        "delete",
                        "--force",
                        sandbox.container_id,
                    )
                # runsc delete removes its filestore from the merged rootfs.
                # Do this before detaching the overlay so that the sealed
                # layer contains the final, cleaned-up filesystem state.
                with self.telemetry.span("sandbox.park.release_storage"):
                    self.rootfs_lifecycle.park_sandbox(sandbox)
                    self.storage.ensure_released(
                        self._storage_owner(sandbox),
                        operation_id=f"{operation_id}:storage-release",
                    )
                with self.telemetry.span("sandbox.park.commit_journal"):
                    return journal.commit_parked(
                        manifest,
                        operation_id=operation_id,
                        expected_revision=pending.revision,
                    )
            finally:
                handle.close()

    def resume(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
        timings: dict[str, float] | None = None,
    ) -> HibernationRecord:
        timings = timings if timings is not None else {}
        resume_started = time.monotonic()
        with self._locked(sandbox):
            phase = time.monotonic()
            journal = self._journal(sandbox)
            parked = self._require_state(sandbox, HibernationState.PARKED)
            try:
                self._mount_storage(
                    sandbox,
                    operation_id=f"{operation_id}:storage-mount",
                )
                self.rootfs_lifecycle.resume_sandbox(sandbox)
                manifest = self.artifacts.load_complete(
                    sandbox_id=sandbox.sandbox_id,
                    sandbox_generation=sandbox.sandbox_generation,
                    hibernation_generation=parked.hibernation_generation,
                )
                manifest.validate_identity(
                    sandbox_id=sandbox.sandbox_id,
                    sandbox_generation=sandbox.sandbox_generation,
                    spec_sha256=sandbox.spec_sha256,
                    runtime_sha256=self._runtime_fingerprint(sandbox).digest,
                )
                # Storage-native resume mounts a new destination-local view above.
                # Bind that exact rootfs ledger to the checkpoint before runsc is
                # allowed to construct or resume any workload task.
                self._require_managed_process_ledger(sandbox, manifest)
            except Exception:
                self._rollback_parked_storage_mount(
                    sandbox,
                    operation_seed=f"{operation_id}:pre-restore-rollback",
                )
                raise
            timings["validate_artifact"] = (time.monotonic() - phase) * 1000
            phase = time.monotonic()
            restoring = journal.begin_restore(
                operation_id=operation_id,
                expected_revision=parked.revision,
            )
            timings["begin_restore_journal"] = (time.monotonic() - phase) * 1000
            generation = self.artifacts.generation_path(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                hibernation_generation=manifest.hibernation_generation,
            )
            candidate: ProcessHandle | None = None
            candidate_record: HibernationRecord | None = None
            try:
                phase = time.monotonic()
                self._checked(
                    *self._common(),
                    "restore",
                    "--detach",
                    "--background",
                    "--cpu-startup-burst",
                    "--start-paused",
                    f"--image-path={generation}",
                    f"--bundle={sandbox.bundle}",
                    sandbox.container_id,
                )
                timings["runsc_restore"] = (time.monotonic() - phase) * 1000
                phase = time.monotonic()
                pid, ticks, status = self._state_identity_status(sandbox)
                timings["runsc_state"] = (time.monotonic() - phase) * 1000
                if status != "paused":
                    raise DirectWardenError("runsc restore candidate was not paused")
                phase = time.monotonic()
                candidate = self.fencer.open(pid, ticks)
                timings["candidate_fence"] = (time.monotonic() - phase) * 1000
                phase = time.monotonic()
                candidate_record = journal.mark_candidate_started(
                    operation_id=operation_id,
                    expected_revision=restoring.revision,
                    candidate_pid=pid,
                    candidate_start_time_ticks=ticks,
                )
                timings["candidate_journal"] = (time.monotonic() - phase) * 1000
                phase = time.monotonic()
                self._ensure_candidate_running(
                    sandbox,
                    expected_pid=pid,
                    expected_start_time_ticks=ticks,
                    known_status=status,
                )
                timings["candidate_resume"] = (time.monotonic() - phase) * 1000
                phase = time.monotonic()
                self._checked(
                    *self._state_prefix(),
                    "exec",
                    sandbox.container_id,
                    *self._readiness_command(sandbox),
                )
                timings["readiness_exec"] = (time.monotonic() - phase) * 1000
            except Exception:
                self._rollback_restore(
                    sandbox,
                    journal=journal,
                    restoring=restoring,
                    candidate=candidate,
                    operation_seed=f"{operation_id}:restore-rollback",
                )
                raise
            finally:
                if candidate is not None:
                    candidate.close()
            assert candidate_record is not None
            phase = time.monotonic()
            running = journal.commit_running(
                operation_id=operation_id,
                expected_revision=candidate_record.revision,
                sentry_pid=pid,
                sentry_start_time_ticks=ticks,
            )
            timings["commit_running_journal"] = (time.monotonic() - phase) * 1000
            # Cleanup is ancillary after RUNNING commits. The paused handoff
            # fenced the candidate before it was allowed to run after consuming
            # the single-owner source.
            phase = time.monotonic()
            try:
                self._finalize_restore_artifacts(manifest)
            except Exception:
                _LOG.exception(
                    "could not remove consumed hibernation generation for %s",
                    sandbox.sandbox_id,
                )
            timings["artifact_cleanup"] = (time.monotonic() - phase) * 1000
            timings["warden_total"] = (time.monotonic() - resume_started) * 1000
            return running

    def reconcile(self, sandbox: DirectSandbox) -> HibernationRecord:
        """Finish or roll back an interrupted lifecycle transition."""
        with self._locked(sandbox):
            journal = self._journal(sandbox)
            durable = journal.load()
            if durable is not None and durable.state != HibernationState.RUNNING:
                self._mount_storage(
                    sandbox,
                    operation_id=f"reconcile:{durable.revision}:storage-mount",
                )
                self.rootfs_lifecycle.resume_sandbox(sandbox)
            if durable is not None and durable.state == HibernationState.RESTORING:
                return self._reconcile_restoring(sandbox, journal, durable)
            result = HibernationReconciler(
                journal,
                self.artifacts,
                runtime_sha256=self._runtime_fingerprint(sandbox).digest,
                proc_root=self.config.proc_root,
                candidate_identity_resolver=lambda _record: (
                    self._candidate_identity_or_none(sandbox)
                ),
            ).reconcile()
            record = result.record
            if (
                result.action == HibernationRecoveryAction.ADOPT_RUNNING
                and record.state == HibernationState.RUNNING
            ):
                self._cleanup_running_restore_artifacts(sandbox, record)
                return record

            if result.action == HibernationRecoveryAction.FINISH_PUBLISHED_GENERATION:
                if record.sentry_pid is None or record.sentry_start_time_ticks is None:
                    raise DirectWardenError(
                        "published capture has no live sentry identity"
                    )
                manifest = self.artifacts.load_complete(
                    sandbox_id=sandbox.sandbox_id,
                    sandbox_generation=sandbox.sandbox_generation,
                    hibernation_generation=record.hibernation_generation,
                )
                handle = self.fencer.open(
                    record.sentry_pid,
                    record.sentry_start_time_ticks,
                )
                try:
                    handle.terminate(timeout=self.config.stop_timeout_seconds)
                finally:
                    handle.close()
                pending = journal.mark_sentry_reaped(
                    operation_id=record.operation_id,
                    expected_revision=record.revision,
                )
                self._checked(
                    *self._state_prefix(),
                    "delete",
                    "--force",
                    sandbox.container_id,
                )
                parked = journal.commit_parked(
                    manifest,
                    operation_id=record.operation_id,
                    expected_revision=pending.revision,
                )
                self._persist_parked_manifest(sandbox, manifest)
                self._release_parked_storage(
                    sandbox,
                    operation_seed=f"reconcile:{parked.revision}",
                )
                return parked

            if result.action == HibernationRecoveryAction.RESUME_OR_RETRY_HIBERNATE:
                if record.sentry_pid is None or record.sentry_start_time_ticks is None:
                    raise DirectWardenError(
                        "interrupted capture has no live sentry identity"
                    )
                handle = self.fencer.open(
                    record.sentry_pid,
                    record.sentry_start_time_ticks,
                )
                try:
                    generation = self.artifacts.generation_path(
                        sandbox_id=sandbox.sandbox_id,
                        sandbox_generation=sandbox.sandbox_generation,
                        hibernation_generation=record.hibernation_generation,
                    )
                    if (generation / _APPLICATION_MEMORY).exists():
                        self._checked(
                            *self._state_prefix(),
                            "resume",
                            sandbox.container_id,
                        )
                    pid, ticks = self._state_identity(sandbox)
                    running = journal.abort_hibernate(
                        operation_id=record.operation_id,
                        expected_revision=record.revision,
                        sentry_pid=pid,
                        sentry_start_time_ticks=ticks,
                    )
                    self.artifacts.discard_pending(
                        sandbox_id=sandbox.sandbox_id,
                        sandbox_generation=sandbox.sandbox_generation,
                        hibernation_generation=record.hibernation_generation,
                    )
                    return running
                finally:
                    handle.close()

            if result.action == HibernationRecoveryAction.FINISH_PENDING_GENERATION:
                return journal.quarantine(
                    reason="sentry died before a complete generation was published",
                    expected_revision=record.revision,
                )

            if record.state == HibernationState.PARKED:
                self._release_parked_storage(
                    sandbox,
                    operation_seed=f"reconcile:{record.revision}",
                )
            return record

    def _reconcile_restoring(
        self,
        sandbox: DirectSandbox,
        journal: HibernationJournal,
        restoring: HibernationRecord,
    ) -> HibernationRecord:
        """Resolve a restore without settling PARKED ahead of COW discard."""
        manifest = self.artifacts.load_published_metadata(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            hibernation_generation=restoring.hibernation_generation,
        )
        manifest.validate_identity(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            spec_sha256=sandbox.spec_sha256,
            runtime_sha256=self._runtime_fingerprint(sandbox).digest,
        )
        if manifest.metadata_sha256 != restoring.manifest_sha256:
            raise DirectWardenError(
                "restore generation does not match the lifecycle journal"
            )

        candidate_identity: tuple[int, int] | None = None
        if (
            restoring.candidate_pid is not None
            and restoring.candidate_start_time_ticks is not None
            and hibernation_process_identity_matches(
                restoring.candidate_pid,
                restoring.candidate_start_time_ticks,
                proc_root=self.config.proc_root,
            )
        ):
            candidate_identity = (
                restoring.candidate_pid,
                restoring.candidate_start_time_ticks,
            )
        elif restoring.authority == HibernationAuthority.PARKED:
            candidate_identity = self._candidate_identity_or_none(sandbox)
            if candidate_identity is not None:
                restoring = journal.mark_candidate_started(
                    operation_id=restoring.operation_id,
                    expected_revision=restoring.revision,
                    candidate_pid=candidate_identity[0],
                    candidate_start_time_ticks=candidate_identity[1],
                )

        if candidate_identity is None:
            return self._rollback_restore(
                sandbox,
                journal=journal,
                restoring=restoring,
                candidate=None,
                operation_seed=f"reconcile:{restoring.revision}:restore-rollback",
                candidate_confirmed_dead=True,
            )

        candidate = self.fencer.open(*candidate_identity)
        try:
            self._ensure_candidate_running(
                sandbox,
                expected_pid=candidate_identity[0],
                expected_start_time_ticks=candidate_identity[1],
            )
            self._checked(
                *self._state_prefix(),
                "exec",
                sandbox.container_id,
                *self._readiness_command(sandbox),
            )
            running = journal.commit_running(
                operation_id=restoring.operation_id,
                expected_revision=restoring.revision,
                sentry_pid=candidate_identity[0],
                sentry_start_time_ticks=candidate_identity[1],
            )
            try:
                self._finalize_restore_artifacts(manifest)
            except Exception:
                _LOG.exception(
                    "could not finalize reconciled restore for %s",
                    sandbox.sandbox_id,
                )
            return running
        except Exception:
            self._rollback_restore(
                sandbox,
                journal=journal,
                restoring=restoring,
                candidate=candidate,
                operation_seed=f"reconcile:{restoring.revision}:restore-rollback",
            )
            raise
        finally:
            candidate.close()

    def delete(self, sandbox: DirectSandbox) -> None:
        """Fence one backend; the storage authority removes its opaque volume."""
        snapshot = self.inspect(sandbox)
        if snapshot is not None and snapshot.state in {
            HibernationState.HIBERNATING,
            HibernationState.RESTORING,
        }:
            self.reconcile(sandbox)
        with self._locked(sandbox):
            journal = self._journal(sandbox)
            record = journal.load()
            if record is None:
                self._parked_manifest_path(sandbox).unlink(missing_ok=True)
                return
            if record.state not in {
                HibernationState.RUNNING,
                HibernationState.PARKED,
                HibernationState.RECOVERY_REQUIRED,
            }:
                raise DirectWardenError(
                    "sandbox transition must be reconciled before deletion"
                )
            if record.authority in {
                HibernationAuthority.LIVE,
                HibernationAuthority.CANDIDATE,
            }:
                pid = (
                    record.sentry_pid
                    if record.authority == HibernationAuthority.LIVE
                    else record.candidate_pid
                )
                ticks = (
                    record.sentry_start_time_ticks
                    if record.authority == HibernationAuthority.LIVE
                    else record.candidate_start_time_ticks
                )
                if pid is None or ticks is None:
                    raise DirectWardenError(
                        "live delete authority lacks a process identity"
                    )
                if hibernation_process_identity_matches(
                    pid,
                    ticks,
                    proc_root=self.config.proc_root,
                ):
                    handle = self.fencer.open(pid, ticks)
                    try:
                        handle.terminate(timeout=self.config.stop_timeout_seconds)
                    finally:
                        handle.close()
                    self._checked(
                        *self._state_prefix(),
                        "delete",
                        "--force",
                        sandbox.container_id,
                    )

            # The storage-native quota owner deletes the opaque volume after
            # this lifecycle fence is removed. Do not remount or traverse it:
            # a backend restart can leave an old ublk mount returning EIO, and
            # deletion must remain possible precisely in that recovery case.
            self.journals.remove(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                expected_revision=record.revision,
                processes_confirmed_dead=True,
            )
            self._parked_manifest_path(sandbox).unlink(missing_ok=True)

    def discard_unjournaled(self, sandbox: DirectSandbox) -> None:
        """Fence an interrupted create before it acquired durable Warden state."""
        with self._locked(sandbox):
            if self._journal(sandbox).load() is not None:
                raise DirectWardenError(
                    "refusing to discard a backend with a lifecycle journal"
                )
            self._best_effort_delete(sandbox)

    def _rollback_capture(
        self,
        sandbox: DirectSandbox,
        *,
        journal: HibernationJournal,
        hibernating: HibernationRecord,
        generation: Path,
        handle: ProcessHandle,
    ) -> None:
        if not handle.alive():
            journal.quarantine(
                reason="sentry died before hibernation publication",
                expected_revision=hibernating.revision,
                live_process_confirmed_dead=True,
            )
            return
        captured_memory = generation / _APPLICATION_MEMORY
        if captured_memory.exists():
            self._checked(
                *self._state_prefix(),
                "resume",
                sandbox.container_id,
            )
        pid, ticks = self._state_identity(sandbox)
        journal.abort_hibernate(
            operation_id=hibernating.operation_id,
            expected_revision=hibernating.revision,
            sentry_pid=pid,
            sentry_start_time_ticks=ticks,
        )
        self.artifacts.discard_pending(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            hibernation_generation=hibernating.hibernation_generation,
        )

    def _rollback_restore(
        self,
        sandbox: DirectSandbox,
        *,
        journal: HibernationJournal,
        restoring: HibernationRecord,
        candidate: ProcessHandle | None,
        operation_seed: str,
        candidate_confirmed_dead: bool = False,
    ) -> HibernationRecord:
        opened_candidate = False
        if candidate is None and not candidate_confirmed_dead:
            try:
                identity = self._candidate_identity_or_none(sandbox)
                if identity is not None:
                    candidate = self.fencer.open(*identity)
                    opened_candidate = True
            except Exception as exc:
                raise DirectWardenError(
                    "cannot prove the restore candidate is fenced"
                ) from exc
        try:
            if candidate is not None and candidate.alive():
                candidate.terminate(timeout=self.config.stop_timeout_seconds)
            self._best_effort_delete(sandbox)
            self._rollback_parked_storage_mount(
                sandbox,
                operation_seed=operation_seed,
            )
            current = journal.load()
            if current is None:
                raise DirectWardenError("restore journal disappeared")
            return journal.rollback_restore(
                operation_id=restoring.operation_id,
                expected_revision=current.revision,
                candidate_reaped=True,
            )
        finally:
            if opened_candidate and candidate is not None:
                candidate.close()

    def _ensure_candidate_running(
        self,
        sandbox: DirectSandbox,
        *,
        expected_pid: int,
        expected_start_time_ticks: int,
        known_status: str | None = None,
    ) -> None:
        status = known_status
        if status is None:
            pid, ticks, status = self._state_identity_status(sandbox)
            if (pid, ticks) != (expected_pid, expected_start_time_ticks):
                raise DirectWardenError(
                    "restore candidate identity changed before resume"
                )
        if status == "paused":
            self._checked(
                *self._state_prefix(),
                "resume",
                sandbox.container_id,
            )
            pid, ticks, status = self._state_identity_status(sandbox)
            if (pid, ticks) != (expected_pid, expected_start_time_ticks):
                raise DirectWardenError(
                    "restore candidate identity changed while resuming"
                )
        if status != "running":
            raise DirectWardenError(
                f"restore candidate did not become running: {status}"
            )

    def _finalize_restore_artifacts(
        self,
        manifest: HibernationManifest,
    ) -> None:
        self.artifacts.delete_published(
            manifest,
            allow_consumed_main_memory=True,
        )

    def _cleanup_running_restore_artifacts(
        self,
        sandbox: DirectSandbox,
        record: HibernationRecord,
    ) -> None:
        """Finish ancillary cleanup after a crash past RUNNING commit."""
        for item in self.artifacts.inventory_incarnation(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            ignored_entries=(
                "upper",
                "work",
                _APPLICATION_MEMORY,
                _ACTIVE_APPLICATION_MEMORY,
            ),
        ):
            if item.hibernation_generation > record.hibernation_generation:
                raise DirectWardenError(
                    "restore artifact generation is ahead of running journal"
                )
            if item.state != "complete":
                raise DirectWardenError(
                    "running sandbox owns an incomplete restore generation"
                )
            manifest = self.artifacts.load_published_metadata(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                hibernation_generation=item.hibernation_generation,
            )
            self.artifacts.delete_published(
                manifest,
                allow_consumed_main_memory=True,
            )

    def _manifest(
        self,
        sandbox: DirectSandbox,
        record: HibernationRecord,
        generation: Path,
    ) -> HibernationManifest:
        roles = {
            _APPLICATION_MEMORY: HibernationFileRole.MAIN_MEMORY,
            _CHECKPOINT_STATE: HibernationFileRole.KERNEL_STATE,
            _PAGES_METADATA: HibernationFileRole.ALLOCATOR_METADATA,
            _PRIVATE_PAGES: HibernationFileRole.PRIVATE_PAGES,
        }
        files: list[LocalHibernationArtifactFile] = []
        names = {
            path.name
            for path in generation.iterdir()
            if path.is_file() and not path.name.startswith(".")
        }
        unexpected = names - set(roles)
        if unexpected:
            raise DirectWardenError(
                f"checkpoint contains unsupported files: {sorted(unexpected)}"
            )
        for name, role in roles.items():
            path = generation / name
            if path.exists():
                files.append(LocalHibernationArtifactFile.from_path(path, role=role))
        return HibernationManifest(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            hibernation_generation=record.hibernation_generation,
            operation_id=record.operation_id,
            spec_sha256=sandbox.spec_sha256,
            container_id=sandbox.container_id,
            created_ns=time.time_ns(),
            runtime=self._runtime_fingerprint(sandbox),
            files=tuple(files),
            managed_process_sha256=self._managed_process_ledger_digest(sandbox),
        )

    def _require_managed_process_ledger(
        self,
        sandbox: DirectSandbox,
        manifest: HibernationManifest,
    ) -> None:
        actual = self._managed_process_ledger_digest(sandbox)
        if actual != manifest.managed_process_sha256:
            raise DirectWardenError(
                "managed-process ledger does not match the checkpoint manifest"
            )

    @staticmethod
    def _managed_process_ledger_digest(sandbox: DirectSandbox) -> str:
        try:
            config_payload = (sandbox.bundle / "config.json").read_bytes()
            if len(config_payload) > 1024 * 1024:
                raise ValueError("OCI config is too large")
            config = json.loads(config_payload)
            annotations = config.get("annotations")
            managed = (
                annotations.get("dev.ucloud-sandboxes.managed-process")
                if isinstance(annotations, dict)
                else None
            )
        except FileNotFoundError as exc:
            raise DirectWardenError(
                "sandbox OCI config is absent while verifying managed processes"
            ) from exc
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DirectWardenError(
                "could not verify managed-process OCI identity"
            ) from exc
        if managed is None:
            return ""
        if managed != "v1":
            raise DirectWardenError("managed-process OCI identity is invalid")
        ledger = sandbox.bundle / "rootfs" / ".ucloud-managed" / "state.json"
        try:
            info = ledger.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or stat.S_ISLNK(info.st_mode)
                or info.st_size < 1
                or info.st_size > 1024 * 1024
            ):
                raise ValueError("managed-process ledger file is invalid")
            payload = ledger.read_bytes()
            record = json.loads(payload)
            if (
                not isinstance(record, dict)
                or record.get("version") != 1
                or not isinstance(record.get("job_id"), str)
                or not re.fullmatch(
                    r"[0-9a-f]{64}", str(record.get("spec_sha256") or "")
                )
                or int(record.get("sequence") or 0) < 1
            ):
                raise ValueError("managed-process ledger contents are invalid")
        except FileNotFoundError:
            return hashlib.sha256(b"managed-primary-v1:no-job").hexdigest()
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DirectWardenError("managed-process ledger is unavailable") from exc
        return hashlib.sha256(payload).hexdigest()

    def _runtime_fingerprint(
        self,
        sandbox: DirectSandbox,
    ) -> HibernationRuntimeFingerprint:
        return replace(
            self.config.runtime_fingerprint,
            rootfs_sha256=sandbox.rootfs_sha256,
        )

    def _state_identity_status(
        self,
        sandbox: DirectSandbox,
    ) -> tuple[int, int, str]:
        result = self._checked(
            *self._state_prefix(),
            "state",
            sandbox.container_id,
        )
        try:
            payload = json.loads(result.stdout)
            pid = int(payload["pid"])
            status = str(payload["status"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DirectWardenError("runsc state returned invalid JSON") from exc
        if status not in {"running", "paused"}:
            raise DirectWardenError(f"runsc state is not live: {status}")
        try:
            ticks = linux_process_start_time_ticks(
                pid,
                proc_root=self.config.proc_root,
            )
        except (ProcessLookupError, ValueError) as exc:
            raise DirectWardenError("cannot read sentry process identity") from exc
        return pid, ticks, status

    def _state_identity(self, sandbox: DirectSandbox) -> tuple[int, int]:
        pid, ticks, _status = self._state_identity_status(sandbox)
        return pid, ticks

    def _candidate_identity_or_none(
        self,
        sandbox: DirectSandbox,
    ) -> tuple[int, int] | None:
        state_command = (
            *self._state_prefix(),
            "state",
            sandbox.container_id,
        )
        result = self.runner.run(
            state_command,
            timeout=self.config.command_timeout_seconds,
        )
        if result.returncode != 0:
            listed = self._checked(
                *self._state_prefix(),
                "list",
                "--format=json",
            )
            try:
                inventory = json.loads(listed.stdout)
            except json.JSONDecodeError as exc:
                raise DirectWardenError("runsc list returned invalid JSON") from exc
            if not isinstance(inventory, list) or any(
                not isinstance(item, dict) or not isinstance(item.get("id"), str)
                for item in inventory
            ):
                raise DirectWardenError("runsc list returned invalid JSON")
            if not any(item["id"] == sandbox.container_id for item in inventory):
                return None
            raise DirectWardenError("runsc state failed for a listed restore candidate")
        try:
            payload = json.loads(result.stdout)
            pid = int(payload["pid"])
            status = str(payload["status"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DirectWardenError("runsc state returned invalid JSON") from exc
        if status in {"absent", "stopped"}:
            return None
        if status not in {"running", "paused"}:
            raise DirectWardenError(f"runsc state is not recognized: {status}")
        try:
            ticks = linux_process_start_time_ticks(
                pid,
                proc_root=self.config.proc_root,
            )
        except ProcessLookupError:
            return None
        except ValueError as exc:
            raise DirectWardenError("cannot read sentry process identity") from exc
        return pid, ticks

    def _common(self) -> tuple[str, ...]:
        return (
            str(self.config.runsc),
            f"--root={self.config.runtime_root}",
            "--platform=systrap",
            f"--network={self.config.network}",
            f"--application-memory-file-dir={self.config.memory_root}",
            "--allow-connected-on-save=true",
        )

    def _state_prefix(self) -> tuple[str, ...]:
        return (
            str(self.config.runsc),
            f"--root={self.config.runtime_root}",
        )

    def _checked(self, *argv: str) -> CommandResult:
        result = self.runner.run(
            argv,
            timeout=self.config.command_timeout_seconds,
        )
        if result.returncode != 0:
            raise DirectWardenError(
                f"command failed ({result.returncode}): {result.argv!r}; "
                f"stdout={result.stdout!r}; stderr={result.stderr!r}"
            )
        return result

    def _best_effort_delete(self, sandbox: DirectSandbox) -> None:
        self.runner.run(
            (
                *self._state_prefix(),
                "delete",
                "--force",
                sandbox.container_id,
            ),
            timeout=self.config.command_timeout_seconds,
        )

    def _storage_record(self, sandbox: DirectSandbox) -> StorageVolumeRecord:
        record = self.storage.get_volume(sandbox.memory_directory)
        self._validate_storage_record(sandbox, record)
        return record

    def storage_records_snapshot(
        self,
        sandboxes: Sequence[DirectSandbox],
    ) -> dict[str, StorageVolumeRecord]:
        """Resolve sandbox storage ownership with at most one daemon RPC."""

        expected: dict[str, DirectSandbox] = {}
        for sandbox in sandboxes:
            if sandbox.memory_directory in expected:
                raise DirectWardenError(
                    "direct registry contains duplicate storage-native ownership"
                )
            expected[sandbox.memory_directory] = sandbox
        if not expected:
            return {}
        by_volume: dict[str, StorageVolumeRecord] = {}
        for record in self.storage.list_volumes():
            if record.volume_id in by_volume:
                raise DirectWardenError(
                    "storage-native service returned duplicate volume ownership"
                )
            by_volume[record.volume_id] = record
        snapshot: dict[str, StorageVolumeRecord] = {}
        for volume_id, sandbox in expected.items():
            record = by_volume.get(volume_id)
            if record is None:
                raise DirectWardenError(
                    "storage-native volume does not own this sandbox incarnation"
                )
            self._validate_storage_record(sandbox, record)
            snapshot[volume_id] = record
        return snapshot

    def _validate_storage_record(
        self,
        sandbox: DirectSandbox,
        record: StorageVolumeRecord,
    ) -> None:
        if record.owner != self._storage_owner(sandbox) or Path(
            record.mount_path
        ) != self._active_memory_root(sandbox):
            raise DirectWardenError(
                "storage-native volume does not own this sandbox incarnation"
            )

    @staticmethod
    def _storage_owner(sandbox: DirectSandbox) -> StorageVolumeOwner:
        return StorageVolumeOwner(
            volume_id=sandbox.memory_directory,
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
        )

    def _mount_storage(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> StorageVolumeRecord:
        return self.storage.ensure_mounted(
            self._storage_owner(sandbox),
            operation_id=operation_id,
        )

    def _release_parked_storage(
        self,
        sandbox: DirectSandbox,
        *,
        operation_seed: str,
    ) -> None:
        record = self._storage_record(sandbox)
        if record.state == StorageVolumeState.MOUNTED:
            self.rootfs_lifecycle.park_sandbox(sandbox)
        self.storage.ensure_released(
            self._storage_owner(sandbox),
            operation_id=f"{operation_seed}:storage-release",
        )

    def _rollback_parked_storage_mount(
        self,
        sandbox: DirectSandbox,
        *,
        operation_seed: str,
    ) -> None:
        """Discard a failed restore's uncommitted COW and stay parked.

        A wake mounts either the node-local released snapshot or a published
        snapshot.  Until RUNNING commits, that new upper layer has no durable
        authority and must not survive a failed validation or restore attempt.
        """

        record = self._storage_record(sandbox)
        if record.state == StorageVolumeState.MOUNTED:
            self.rootfs_lifecycle.park_sandbox(sandbox)
        record = self.storage.discard_resume(
            self._storage_owner(sandbox),
            operation_id=f"{operation_seed}:storage-discard",
        )
        if record.state not in {
            StorageVolumeState.RELEASED,
            StorageVolumeState.PUBLISHED,
        }:
            raise DirectWardenError(
                "storage-native restore rollback returned invalid authority"
            )

    def _journal(self, sandbox: DirectSandbox) -> HibernationJournal:
        return self.journals.journal(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
        )

    def _parked_manifest_path(self, sandbox: DirectSandbox) -> Path:
        return (
            self.config.runtime_root
            / "parked-manifests"
            / f"{sandbox.sandbox_id}.sandbox-{sandbox.sandbox_generation}.json"
        )

    def _persist_parked_manifest(
        self,
        sandbox: DirectSandbox,
        manifest: HibernationManifest,
    ) -> None:
        manifest.validate_identity(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            spec_sha256=sandbox.spec_sha256,
            runtime_sha256=self._runtime_fingerprint(sandbox).digest,
        )
        target = self._parked_manifest_path(sandbox)
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        payload = (
            json.dumps(
                manifest.to_dict(),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
            + b"\n"
        )
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.",
            dir=target.parent,
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(target)
            directory = os.open(
                target.parent,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except Exception:
            try:
                os.close(descriptor)
            except OSError:
                pass
            temporary.unlink(missing_ok=True)
            raise

    def _require_state(
        self,
        sandbox: DirectSandbox,
        expected: HibernationState,
    ) -> HibernationRecord:
        record = self._journal(sandbox).load()
        if record is None:
            raise DirectWardenError("sandbox has no Warden lifecycle journal")
        if record.state != expected:
            raise DirectWardenError(
                f"sandbox is {record.state.value}, expected {expected.value}"
            )
        return record

    def _active_memory_root(self, sandbox: DirectSandbox) -> Path:
        path = self.config.memory_root / sandbox.memory_directory
        if path.parent != self.config.memory_root:
            raise DirectWardenError("active memory directory escaped its root")
        return path

    def _validate_bundle(self, sandbox: DirectSandbox) -> None:
        try:
            bundle = sandbox.bundle.resolve(strict=True)
            root = self.config.bundle_root.resolve(strict=True)
            bundle.relative_to(root)
        except (OSError, ValueError) as exc:
            raise DirectWardenError(
                "sandbox bundle must be a durable directory below bundle_root"
            ) from exc
        self._require_private_directory(bundle, "sandbox bundle")
        config_path = bundle / "config.json"
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            configured = payload["annotations"][
                "dev.gvisor.internal.application-memory-directory"
            ]
        except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
            raise DirectWardenError(
                "bundle lacks a valid application-memory-directory annotation"
            ) from exc
        if configured != sandbox.memory_directory:
            raise DirectWardenError(
                "bundle application-memory-directory does not match Warden state"
            )

    def _ensure_roots(self) -> None:
        for path in (
            self.config.runtime_root,
            self.config.runtime_root / "warden-locks",
            self.config.runtime_root / "parked-manifests",
            self.config.memory_root,
            self.config.bundle_root,
            self.config.journal_root,
        ):
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
            self._require_private_directory(path, "Warden root")

    @staticmethod
    def _require_private_directory(path: Path, label: str) -> None:
        info = path.lstat()
        if not path.is_dir() or path.is_symlink():
            raise DirectWardenError(f"{label} must be a real directory")
        if info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise DirectWardenError(
                f"{label} must be owned and not group/world writable"
            )

    @contextmanager
    def _locked(self, sandbox: DirectSandbox) -> Iterator[None]:
        lock_path = (
            self.config.runtime_root
            / "warden-locks"
            / f".{sandbox.sandbox_id}.sandbox-{sandbox.sandbox_generation}.warden.lock"
        )
        descriptor = os.open(
            lock_path,
            os.O_RDWR
            | os.O_CREAT
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)
