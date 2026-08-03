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
    HibernationArtifactFile,
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
    hibernation_process_identity_matches,
    linux_process_start_time_ticks,
)
from .storage_native_daemon import StorageNativeNodeClient


_SAFE_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_CONTAINER_ID = re.compile(r"[0-9a-f]{64}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_LOG = logging.getLogger(__name__)
_APPLICATION_MEMORY = "application_memory.img"
_ACTIVE_APPLICATION_MEMORY = "application_memory.active"
_CHECKPOINT_STATE = "checkpoint.img"
_PAGES_METADATA = "pages_meta.img"
_PRIVATE_PAGES = "pages.img"
_RESTORE_IMAGE = ".restore-image"
_RESTORE_SOURCE = ".source.json"
_FICLONE = 0x40049409


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
    artifact_root: Path
    runtime_fingerprint: HibernationRuntimeFingerprint
    proc_root: Path = Path("/proc")
    platform: str = "systrap"
    network: str = "none"
    command_timeout_seconds: float = 60.0
    stop_timeout_seconds: float = 30.0
    restore_background: bool = True
    restore_cpu_startup_burst: bool = False
    # A reflink restore is only quota-safe when the restored guest cannot dirty
    # shared extents until the authoritative checkpoint generation is gone.
    restore_reflink: bool = False
    # A reflink has a distinct inode and therefore a cold page-cache identity.
    # Ask the kernel to begin reading the candidate's main-memory image while
    # runsc restores allocator metadata.
    restore_prefetch_memory: bool = False
    restore_start_paused: bool = False
    allow_connected_on_save: bool = True
    readiness_command: tuple[str, ...] = ("/bin/true",)
    remove_memory_directory_on_delete: bool = True

    def __post_init__(self) -> None:
        for label, path in (
            ("runsc", self.runsc),
            ("runtime_root", self.runtime_root),
            ("memory_root", self.memory_root),
            ("bundle_root", self.bundle_root),
            ("journal_root", self.journal_root),
            ("artifact_root", self.artifact_root),
            ("proc_root", self.proc_root),
        ):
            if not path.is_absolute():
                raise ValueError(f"{label} must be absolute")
        if self.command_timeout_seconds <= 0 or self.stop_timeout_seconds <= 0:
            raise ValueError("Warden timeouts must be positive")
        if not self.readiness_command:
            raise ValueError("readiness_command cannot be empty")
        if self.restore_reflink and not self.restore_start_paused:
            raise ValueError(
                "restore_reflink requires restore_start_paused for hard-quota safety"
            )
        if self.restore_prefetch_memory and not self.restore_reflink:
            raise ValueError("restore_prefetch_memory requires restore_reflink")


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
        storage: StorageNativeNodeClient | None = None,
        rootfs_lifecycle: RootfsMountLifecycle | None = None,
    ) -> None:
        self.config = config
        self.runner = runner or SubprocessCommandRunner()
        self.fencer = fencer or LinuxPidfdFencer(proc_root=config.proc_root)
        self.storage = storage
        self.rootfs_lifecycle = rootfs_lifecycle
        if (storage is None) != (rootfs_lifecycle is None):
            raise ValueError(
                "storage-native Warden requires a rootfs mount lifecycle"
            )
        self.journals = HibernationJournalStore(config.journal_root)
        self.artifacts = HibernationArtifactStore(
            config.artifact_root,
            preserve_incarnation_roots=(
                storage is not None
                and config.artifact_root == config.memory_root
            ),
            require_stable_device=not (
                storage is not None
                and config.artifact_root == config.memory_root
            ),
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
                    raise DirectWardenError(
                        "parked manifest control copy is too large"
                    )
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
            manifest.require_compatible(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                spec_sha256=sandbox.spec_sha256,
                runtime_sha256=self._runtime_fingerprint(sandbox).digest,
            )
            self._require_managed_process_ledger(sandbox, manifest)
            if (
                manifest.hibernation_generation
                != parked.hibernation_generation
                or manifest.metadata_sha256 != parked.manifest_sha256
            ):
                raise DirectWardenError(
                    "parked manifest control copy changed identity"
                )
            return manifest

    def publish_storage_snapshot(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> dict[str, object]:
        if self.storage is None:
            raise DirectWardenError("storage-native service is not configured")
        with self._locked(sandbox):
            lifecycle = self._require_state(sandbox, HibernationState.PARKED)
            if lifecycle.state != HibernationState.PARKED:
                raise DirectWardenError(
                    "only a parked sandbox can publish storage authority"
                )
            record = self._storage_record(sandbox)
            if record.get("state") == "published":
                return record
            if record.get("state") == "mounted":
                assert self.rootfs_lifecycle is not None
                self.rootfs_lifecycle.park_sandbox(sandbox)
                record = self._seal_storage(
                    sandbox,
                    operation_id=f"{operation_id}:seal",
                )
            if record.get("state") == "sealed":
                record = self._release_storage(
                    sandbox,
                    operation_id=f"{operation_id}:release",
                )
            if record.get("state") != "released":
                raise DirectWardenError(
                    f"storage-native volume is {record.get('state')}, "
                    "not publishable"
                )
            result = self.storage.publish_snapshot(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                volume_id=sandbox.memory_directory,
                operation_id=self._storage_operation_id(sandbox, operation_id),
                expected_revision=int(record["revision"]),
            )
            raw = result.get("record")
            if not isinstance(raw, dict) or raw.get("state") != "published":
                raise DirectWardenError(
                    "storage-native publication returned an invalid record"
                )
            return raw

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
            manifest.require_compatible(
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
                if self.storage is not None:
                    # runsc delete removes its filestore from the merged rootfs.
                    # Do this before detaching the overlay so that the sealed
                    # layer contains the final, cleaned-up filesystem state.
                    assert self.rootfs_lifecycle is not None
                    self.rootfs_lifecycle.park_sandbox(sandbox)
                    self._seal_storage(
                        sandbox,
                        operation_id=f"{operation_id}:storage-seal",
                    )
                    self._release_storage(
                        sandbox,
                        operation_id=f"{operation_id}:storage-release",
                    )
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
                if self.storage is not None:
                    self._mount_storage(
                        sandbox,
                        operation_id=f"{operation_id}:storage-mount",
                    )
                    assert self.rootfs_lifecycle is not None
                    self.rootfs_lifecycle.resume_sandbox(sandbox)
                manifest = self.artifacts.load_complete(
                    sandbox_id=sandbox.sandbox_id,
                    sandbox_generation=sandbox.sandbox_generation,
                    hibernation_generation=parked.hibernation_generation,
                )
                manifest.require_compatible(
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
            phase = time.monotonic()
            restore_image, restore_reflinked = self._prepare_restore_image(
                sandbox,
                manifest,
            )
            timings["restore_image_stage"] = (time.monotonic() - phase) * 1000
            timings["restore_image_reflinked"] = float(restore_reflinked)
            candidate: ProcessHandle | None = None
            restore_started = False
            source_dropped = False
            candidate_record: HibernationRecord | None = None
            try:
                restore_flags = ["--detach"]
                if self.config.restore_background:
                    restore_flags.append("--background")
                if self.config.restore_cpu_startup_burst:
                    restore_flags.append("--cpu-startup-burst")
                if self.config.restore_start_paused:
                    restore_flags.append("--start-paused")
                phase = time.monotonic()
                self._checked(
                    *self._common(),
                    "restore",
                    *restore_flags,
                    f"--image-path={restore_image}",
                    f"--bundle={sandbox.bundle}",
                    sandbox.container_id,
                )
                timings["runsc_restore"] = (time.monotonic() - phase) * 1000
                restore_started = True
                phase = time.monotonic()
                pid, ticks, status = self._state_identity_status(sandbox)
                timings["runsc_state"] = (time.monotonic() - phase) * 1000
                if self.config.restore_start_paused and status != "paused":
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
                if restore_reflinked:
                    phase = time.monotonic()
                    self._drop_restore_source(sandbox, manifest)
                    source_dropped = True
                    timings["restore_source_drop"] = (time.monotonic() - phase) * 1000
                if self.config.restore_start_paused:
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
                    *self.config.readiness_command,
                )
                timings["readiness_exec"] = (time.monotonic() - phase) * 1000
            except Exception:
                if not source_dropped and self._restore_source_is_complete(
                    sandbox, restoring
                ):
                    self._rollback_restore(
                        sandbox,
                        journal=journal,
                        restoring=restoring,
                        manifest=manifest,
                        candidate=candidate,
                        restore_started=restore_started,
                        restore_reflinked=restore_reflinked,
                    )
                    self._rollback_parked_storage_mount(
                        sandbox,
                        operation_seed=f"{operation_id}:restore-rollback",
                    )
                else:
                    _LOG.exception(
                        "restore source was dropped for %s; preserving the "
                        "fenced candidate for reconciliation",
                        sandbox.sandbox_id,
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
            # Cleanup is ancillary after RUNNING commits. The quota-relevant
            # source generation was already durably dropped while a reflinked
            # candidate was paused.
            phase = time.monotonic()
            try:
                self._finalize_restore_artifacts(
                    sandbox,
                    manifest,
                    restore_reflinked=restore_reflinked,
                )
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
            if (
                self.storage is not None
                and durable is not None
                and durable.state != HibernationState.RUNNING
            ):
                self._mount_storage(
                    sandbox,
                    operation_id=f"reconcile:{durable.revision}:storage-mount",
                )
                assert self.rootfs_lifecycle is not None
                self.rootfs_lifecycle.resume_sandbox(sandbox)
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
                if self.storage is not None:
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

            if result.action == HibernationRecoveryAction.RETRY_RESTORE:
                self._best_effort_delete(sandbox)
                self._discard_restore_image(
                    sandbox,
                    allow_consumed_main_memory=True,
                )
                parked = journal.rollback_restore(
                    operation_id=record.operation_id,
                    expected_revision=record.revision,
                )
                if self.storage is not None:
                    self._rollback_parked_storage_mount(
                        sandbox,
                        operation_seed=f"reconcile:{parked.revision}",
                    )
                return parked

            if result.action == HibernationRecoveryAction.VERIFY_CANDIDATE:
                if (
                    record.candidate_pid is None
                    or record.candidate_start_time_ticks is None
                ):
                    raise DirectWardenError("restore candidate has no process identity")
                manifest: HibernationManifest | None = None
                if self._restore_source_is_complete(sandbox, record):
                    manifest = self.artifacts.load_published_metadata(
                        sandbox_id=sandbox.sandbox_id,
                        sandbox_generation=sandbox.sandbox_generation,
                        hibernation_generation=record.hibernation_generation,
                    )
                restore_reflinked = (
                    self._restore_image_source_digest(sandbox) == record.manifest_sha256
                )
                candidate = self.fencer.open(
                    record.candidate_pid,
                    record.candidate_start_time_ticks,
                )
                try:
                    if restore_reflinked:
                        if manifest is not None:
                            self._drop_restore_source(sandbox, manifest)
                        else:
                            self._finish_dropped_restore_source(sandbox, record)
                    self._ensure_candidate_running(
                        sandbox,
                        expected_pid=record.candidate_pid,
                        expected_start_time_ticks=(record.candidate_start_time_ticks),
                    )
                    self._checked(
                        *self._state_prefix(),
                        "exec",
                        sandbox.container_id,
                        *self.config.readiness_command,
                    )
                    running = journal.commit_running(
                        operation_id=record.operation_id,
                        expected_revision=record.revision,
                        sentry_pid=record.candidate_pid,
                        sentry_start_time_ticks=record.candidate_start_time_ticks,
                    )
                    try:
                        if manifest is None:
                            self._discard_restore_image(
                                sandbox,
                                allow_consumed_main_memory=True,
                            )
                        else:
                            self._finalize_restore_artifacts(
                                sandbox,
                                manifest,
                                restore_reflinked=restore_reflinked,
                            )
                    except Exception:
                        _LOG.exception(
                            "could not finalize reconciled restore for %s",
                            sandbox.sandbox_id,
                        )
                    return running
                except Exception:
                    if manifest is not None and self._restore_source_is_complete(
                        sandbox, record
                    ):
                        self._rollback_restore(
                            sandbox,
                            journal=journal,
                            restoring=record,
                            manifest=manifest,
                            candidate=candidate,
                            restore_started=True,
                            restore_reflinked=restore_reflinked,
                        )
                    else:
                        _LOG.exception(
                            "restore source was dropped for %s; preserving the "
                            "candidate for the next reconciliation",
                            sandbox.sandbox_id,
                        )
                    raise
                finally:
                    candidate.close()

            if (
                self.storage is not None
                and record.state == HibernationState.PARKED
            ):
                self._release_parked_storage(
                    sandbox,
                    operation_seed=f"reconcile:{record.revision}",
                )
            return record

    def delete(self, sandbox: DirectSandbox) -> None:
        """Delete one settled backend and all of its owned local generations."""
        with self._locked(sandbox):
            journal = self._journal(sandbox)
            record = journal.load()
            if record is None:
                return
            if self.storage is not None and record.state != HibernationState.RUNNING:
                self._mount_storage(
                    sandbox,
                    operation_id=f"delete:{record.revision}:storage-mount",
                )
            # Deletion can be replayed after a crash that already reaped the
            # sentry but did not remove the journal. Reconcile the durable
            # authority first so a dead exact PID becomes recovery-owned
            # cleanup rather than an unrecoverable fencing error.
            record = (
                HibernationReconciler(
                    journal,
                    self.artifacts,
                    runtime_sha256=self._runtime_fingerprint(sandbox).digest,
                    proc_root=self.config.proc_root,
                    candidate_identity_resolver=lambda _record: (
                        self._candidate_identity_or_none(sandbox)
                    ),
                )
                .reconcile()
                .record
            )
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

            unified_storage = self.config.memory_root == self.config.artifact_root
            for item in self.artifacts.inventory_incarnation(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                ignored_entries=(
                    (
                        "upper",
                        "work",
                        _APPLICATION_MEMORY,
                        _ACTIVE_APPLICATION_MEMORY,
                        _RESTORE_IMAGE,
                    )
                    if unified_storage
                    else ()
                ),
            ):
                if item.state == "pending":
                    self.artifacts.discard_pending(
                        sandbox_id=sandbox.sandbox_id,
                        sandbox_generation=sandbox.sandbox_generation,
                        hibernation_generation=item.hibernation_generation,
                    )
                    continue
                manifest = self.artifacts.load_published_metadata(
                    sandbox_id=sandbox.sandbox_id,
                    sandbox_generation=sandbox.sandbox_generation,
                    hibernation_generation=item.hibernation_generation,
                )
                self.artifacts.delete_published(
                    manifest,
                    allow_consumed_main_memory=(
                        record.state == HibernationState.RUNNING
                        or item.hibernation_generation != record.hibernation_generation
                    ),
                )

            self._discard_restore_image(
                sandbox,
                allow_consumed_main_memory=True,
            )
            active_memory = self._active_memory_root(sandbox)
            if active_memory.exists() and self.config.remove_memory_directory_on_delete:
                try:
                    active_memory.rmdir()
                except OSError as exc:
                    raise DirectWardenError(
                        "active memory directory is not empty after runtime delete"
                    ) from exc
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
        manifest: HibernationManifest,
        candidate: ProcessHandle | None,
        restore_started: bool,
        restore_reflinked: bool,
    ) -> None:
        generation = self.artifacts.generation_path(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            hibernation_generation=restoring.hibernation_generation,
        )
        active_memory = self._active_memory_root(sandbox) / _ACTIVE_APPLICATION_MEMORY
        if not restore_started:
            if restore_reflinked:
                self._discard_restore_image(sandbox)
            journal.rollback_restore(
                operation_id=restoring.operation_id,
                expected_revision=restoring.revision,
            )
            return
        opened_candidate = False
        if candidate is None:
            try:
                pid, ticks = self._state_identity(sandbox)
                candidate = self.fencer.open(pid, ticks)
                opened_candidate = True
            except Exception as exc:
                raise DirectWardenError(
                    "cannot prove the restore candidate is fenced"
                ) from exc
        try:
            if candidate is not None and candidate.alive():
                candidate.terminate(timeout=self.config.stop_timeout_seconds)
            if not restore_reflinked and (
                active_memory.exists()
                and not (generation / _APPLICATION_MEMORY).exists()
            ):
                self.artifacts.return_consumed_file(
                    manifest,
                    active_root=active_memory.parent,
                    file_name=active_memory.name,
                    artifact_name=_APPLICATION_MEMORY,
                )
            self._best_effort_delete(sandbox)
            if restore_reflinked:
                self._discard_restore_image(
                    sandbox,
                    allow_consumed_main_memory=True,
                )
            current = journal.load()
            if current is None:
                raise DirectWardenError("restore journal disappeared")
            journal.rollback_restore(
                operation_id=restoring.operation_id,
                expected_revision=current.revision,
                candidate_reaped=True,
            )
        finally:
            if opened_candidate and candidate is not None:
                candidate.close()

    def _restore_source_is_complete(
        self,
        sandbox: DirectSandbox,
        record: HibernationRecord,
    ) -> bool:
        generation = self.artifacts.generation_path(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            hibernation_generation=record.hibernation_generation,
        )
        return os.path.lexists(generation / self.artifacts.COMPLETE_NAME)

    def _drop_restore_source(
        self,
        sandbox: DirectSandbox,
        manifest: HibernationManifest,
    ) -> None:
        """Durably remove checkpoint authority before a CoW guest can run."""
        generation = self.artifacts.generation_path(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            hibernation_generation=manifest.hibernation_generation,
        )
        if os.path.lexists(generation / self.artifacts.COMPLETE_NAME):
            self.artifacts.delete_published(manifest)
            return
        if os.path.lexists(generation):
            # delete_published removes COMPLETE first. A crash after that point
            # leaves a pending generation which can only be finished, never
            # treated as a restorable checkpoint again.
            self.artifacts.discard_pending(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                hibernation_generation=manifest.hibernation_generation,
            )

    def _finish_dropped_restore_source(
        self,
        sandbox: DirectSandbox,
        record: HibernationRecord,
    ) -> None:
        generation = self.artifacts.generation_path(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            hibernation_generation=record.hibernation_generation,
        )
        if os.path.lexists(generation / self.artifacts.COMPLETE_NAME):
            raise DirectWardenError(
                "restore source is still complete but its manifest was not loaded"
            )
        if os.path.lexists(generation):
            self.artifacts.discard_pending(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                hibernation_generation=record.hibernation_generation,
            )

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

    def _restore_image_path(self, sandbox: DirectSandbox) -> Path:
        return self._active_memory_root(sandbox) / _RESTORE_IMAGE

    def _prepare_restore_image(
        self,
        sandbox: DirectSandbox,
        manifest: HibernationManifest,
    ) -> tuple[Path, bool]:
        generation = self.artifacts.generation_path(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            hibernation_generation=manifest.hibernation_generation,
        )
        if not self.config.restore_reflink:
            return generation, False
        restore_image = self._restore_image_path(sandbox)
        self._discard_restore_image(
            sandbox,
            allow_consumed_main_memory=True,
        )
        restore_image.mkdir(mode=0o700)
        self._fsync_directory(restore_image.parent)
        try:
            for item in manifest.files:
                self._reflink_file(
                    generation / item.name,
                    restore_image / item.name,
                )
            if self.config.restore_prefetch_memory:
                self._prefetch_file(restore_image / _APPLICATION_MEMORY)
            marker = {
                "files": [item.name for item in manifest.files],
                "metadata_sha256": manifest.metadata_sha256,
                "version": 1,
            }
            self._atomic_private_json(
                restore_image / _RESTORE_SOURCE,
                marker,
            )
            self._fsync_directory(restore_image)
        except Exception:
            self._discard_restore_image(
                sandbox,
                allow_consumed_main_memory=True,
            )
            _LOG.warning(
                "reflink restore staging unavailable for %s; consuming the "
                "single-owner generation",
                sandbox.sandbox_id,
                exc_info=True,
            )
            return generation, False
        return restore_image, True

    def _restore_image_matches(
        self,
        sandbox: DirectSandbox,
        manifest: HibernationManifest,
    ) -> bool:
        marker_path = self._restore_image_path(sandbox) / _RESTORE_SOURCE
        try:
            raw = marker_path.read_bytes()
            if len(raw) > 1024 * 1024:
                return False
            marker = json.loads(raw)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return False
        return marker == {
            "files": [item.name for item in manifest.files],
            "metadata_sha256": manifest.metadata_sha256,
            "version": 1,
        }

    def _restore_image_source_digest(
        self,
        sandbox: DirectSandbox,
    ) -> str | None:
        marker_path = self._restore_image_path(sandbox) / _RESTORE_SOURCE
        if not os.path.lexists(marker_path):
            return None
        try:
            raw = marker_path.read_bytes()
            if len(raw) > 1024 * 1024:
                raise DirectWardenError("restore source marker is too large")
            marker = json.loads(raw)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DirectWardenError("restore source marker is invalid") from exc
        if (
            not isinstance(marker, dict)
            or set(marker) != {"files", "metadata_sha256", "version"}
            or marker.get("version") != 1
            or not isinstance(marker.get("files"), list)
            or not all(
                isinstance(name, str)
                and name
                in {
                    _APPLICATION_MEMORY,
                    _CHECKPOINT_STATE,
                    _PAGES_METADATA,
                    _PRIVATE_PAGES,
                }
                for name in marker["files"]
            )
            or len(set(marker["files"])) != len(marker["files"])
            or not isinstance(marker.get("metadata_sha256"), str)
            or not _DIGEST.fullmatch(marker["metadata_sha256"])
        ):
            raise DirectWardenError("restore source marker is invalid")
        return marker["metadata_sha256"]

    def _discard_restore_image(
        self,
        sandbox: DirectSandbox,
        *,
        allow_consumed_main_memory: bool = False,
    ) -> None:
        restore_image = self._restore_image_path(sandbox)
        if not os.path.lexists(restore_image):
            return
        if not restore_image.is_dir() or restore_image.is_symlink():
            raise DirectWardenError("restore image must be a real directory")
        allowed = {
            _APPLICATION_MEMORY,
            _CHECKPOINT_STATE,
            _PAGES_METADATA,
            _PRIVATE_PAGES,
            _RESTORE_SOURCE,
        }
        actual = set(os.listdir(restore_image))
        unexpected = actual - allowed
        if unexpected:
            raise DirectWardenError(
                f"restore image contains unexpected entries: {sorted(unexpected)}"
            )
        if (
            not allow_consumed_main_memory
            and _RESTORE_SOURCE in actual
            and _APPLICATION_MEMORY not in actual
        ):
            raise DirectWardenError(
                "restore image memory was consumed before candidate fencing"
            )
        directory_fd = os.open(
            restore_image,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            for name in actual:
                entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                if not stat.S_ISREG(entry.st_mode):
                    raise DirectWardenError(
                        "restore image contains a non-regular entry"
                    )
            for name in sorted(actual):
                os.unlink(name, dir_fd=directory_fd)
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        restore_image.rmdir()
        self._fsync_directory(restore_image.parent)

    def _finalize_restore_artifacts(
        self,
        sandbox: DirectSandbox,
        manifest: HibernationManifest,
        *,
        restore_reflinked: bool,
    ) -> None:
        if restore_reflinked:
            self._drop_restore_source(sandbox, manifest)
            self._discard_restore_image(
                sandbox,
                allow_consumed_main_memory=True,
            )
            return
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
        self._discard_restore_image(
            sandbox,
            allow_consumed_main_memory=True,
        )
        unified_storage = self.config.memory_root == self.config.artifact_root
        for item in self.artifacts.inventory_incarnation(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            ignored_entries=(
                (
                    "upper",
                    "work",
                    _APPLICATION_MEMORY,
                    _ACTIVE_APPLICATION_MEMORY,
                    _RESTORE_IMAGE,
                )
                if unified_storage
                else ()
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

    @staticmethod
    def _reflink_file(source: Path, target: Path) -> None:
        source_fd = os.open(
            source,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        target_fd = -1
        try:
            source_info = os.fstat(source_fd)
            if not stat.S_ISREG(source_info.st_mode):
                raise DirectWardenError("reflink source must be a regular file")
            target_fd = os.open(
                target,
                os.O_RDWR
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            fcntl.ioctl(target_fd, _FICLONE, source_fd)
            os.fsync(target_fd)
        finally:
            if target_fd >= 0:
                os.close(target_fd)
            os.close(source_fd)

    @staticmethod
    def _prefetch_file(path: Path) -> None:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.posix_fadvise(descriptor, 0, 0, os.POSIX_FADV_WILLNEED)
        finally:
            os.close(descriptor)

    @staticmethod
    def _atomic_private_json(path: Path, payload: object) -> None:
        descriptor, raw_path = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        temporary = Path(raw_path)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(
                    json.dumps(
                        payload,
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode("ascii")
                )
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

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
        files: list[HibernationArtifactFile] = []
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
                files.append(HibernationArtifactFile.from_path(path, role=role))
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
        except FileNotFoundError:
            # Legacy non-managed sandboxes and old test fixtures predate this
            # annotation. A managed sandbox always has a generated OCI config.
            return ""
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
            raise DirectWardenError(
                "managed-process ledger is unavailable"
            ) from exc
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
        try:
            return self._state_identity(sandbox)
        except DirectWardenError:
            return None

    def _common(self) -> tuple[str, ...]:
        command = [
            str(self.config.runsc),
            f"--root={self.config.runtime_root}",
            f"--platform={self.config.platform}",
            f"--network={self.config.network}",
            f"--application-memory-file-dir={self.config.memory_root}",
        ]
        if self.config.allow_connected_on_save:
            command.append("--allow-connected-on-save=true")
        return tuple(command)

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

    def _storage_record(self, sandbox: DirectSandbox) -> dict[str, object]:
        if self.storage is None:
            raise DirectWardenError("storage-native service is not configured")
        result = self.storage.get_volume(sandbox.memory_directory)
        raw = result.get("record")
        if not isinstance(raw, dict):
            raise DirectWardenError(
                "storage-native service returned an invalid volume record"
            )
        if (
            raw.get("sandbox_id") != sandbox.sandbox_id
            or raw.get("sandbox_generation") != sandbox.sandbox_generation
            or raw.get("volume_id") != sandbox.memory_directory
            or Path(str(raw.get("mount_path") or ""))
            != self._active_memory_root(sandbox)
        ):
            raise DirectWardenError(
                "storage-native volume does not own this sandbox incarnation"
            )
        return raw

    @staticmethod
    def _storage_operation_id(
        sandbox: DirectSandbox,
        operation_id: str,
    ) -> str:
        """Scope storage-daemon idempotency to one sandbox incarnation.

        The storage operation journal is node-global.  Lifecycle revisions and
        caller request IDs are not necessarily global, so using them directly
        lets unrelated sandboxes collide (for example, both reaching
        ``delete:3:storage-mount``).  A fixed digest remains replay-stable while
        also fitting the daemon's bounded safe-ID grammar.
        """

        identity = (
            f"{sandbox.sandbox_id}\0{sandbox.sandbox_generation}\0{operation_id}"
        )
        return "warden-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()

    def _mount_storage(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> dict[str, object]:
        assert self.storage is not None
        record = self._storage_record(sandbox)
        state = record.get("state")
        if state == "mounted":
            return record
        if state == "sealed":
            released = self.storage.release_runtime(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                volume_id=sandbox.memory_directory,
                operation_id=self._storage_operation_id(
                    sandbox,
                    f"{operation_id}:release-sealed",
                ),
                expected_revision=int(record["revision"]),
            )
            raw_released = released.get("record")
            if not isinstance(raw_released, dict):
                raise DirectWardenError(
                    "storage-native release returned an invalid record"
                )
            record = raw_released
            state = record.get("state")
        if state in {"released", "published"}:
            mounted = self.storage.mount_snapshot_cow(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                volume_id=sandbox.memory_directory,
                operation_id=self._storage_operation_id(
                    sandbox,
                    f"{operation_id}:acquire",
                ),
                expected_revision=int(record["revision"]),
            )
            raw_mounted = mounted.get("record")
            if not isinstance(raw_mounted, dict):
                raise DirectWardenError(
                    "storage-native acquire returned an invalid record"
                )
            return raw_mounted
        raise DirectWardenError(
            f"storage-native volume is {state}, not resumable"
        )

    def _seal_storage(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> dict[str, object]:
        assert self.storage is not None
        record = self._storage_record(sandbox)
        if record.get("state") == "sealed":
            return record
        if record.get("state") != "mounted":
            raise DirectWardenError(
                f"storage-native volume is {record.get('state')}, not sealable"
            )
        sealed = self.storage.freeze_and_seal(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            volume_id=sandbox.memory_directory,
            operation_id=self._storage_operation_id(sandbox, operation_id),
            expected_revision=int(record["revision"]),
        )
        raw = sealed.get("record")
        if not isinstance(raw, dict):
            raise DirectWardenError(
                "storage-native seal returned an invalid record"
            )
        return raw

    def _release_storage(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> dict[str, object]:
        assert self.storage is not None
        record = self._storage_record(sandbox)
        if record.get("state") in {"released", "published"}:
            return record
        if record.get("state") != "sealed":
            raise DirectWardenError(
                f"storage-native volume is {record.get('state')}, not releasable"
            )
        released = self.storage.release_runtime(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            volume_id=sandbox.memory_directory,
            operation_id=self._storage_operation_id(sandbox, operation_id),
            expected_revision=int(record["revision"]),
        )
        raw = released.get("record")
        if not isinstance(raw, dict):
            raise DirectWardenError(
                "storage-native release returned an invalid record"
            )
        return raw

    def _release_parked_storage(
        self,
        sandbox: DirectSandbox,
        *,
        operation_seed: str,
    ) -> None:
        record = self._storage_record(sandbox)
        if record.get("state") == "mounted":
            assert self.rootfs_lifecycle is not None
            self.rootfs_lifecycle.park_sandbox(sandbox)
            self._seal_storage(
                sandbox,
                operation_id=f"{operation_seed}:storage-seal",
            )
        self._release_storage(
            sandbox,
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

        if self.storage is None:
            return
        record = self._storage_record(sandbox)
        if record.get("state") != "mounted":
            return
        assert self.rootfs_lifecycle is not None
        self.rootfs_lifecycle.park_sandbox(sandbox)
        record = self._storage_record(sandbox)
        if record.get("state") != "mounted":
            return
        result = self.storage.discard_mounted_cow(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            volume_id=sandbox.memory_directory,
            operation_id=self._storage_operation_id(
                sandbox,
                f"{operation_seed}:storage-discard",
            ),
            expected_revision=int(record["revision"]),
        )
        raw = result.get("record")
        if not isinstance(raw, dict) or raw.get("state") not in {
            "released",
            "published",
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
        manifest.require_compatible(
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
            self.config.artifact_root,
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
