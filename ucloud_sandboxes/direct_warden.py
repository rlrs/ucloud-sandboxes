from __future__ import annotations

from .checkpoint_components import MemoryBackingRef, WorkspaceCaptureRef
from .memory_backing import MemoryBackingStore, RetainedCheckpointRef

from contextlib import contextmanager
from dataclasses import dataclass, replace
import fcntl
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import select
import signal
import stat
import subprocess
import tempfile
import time
from uuid import UUID
from typing import TYPE_CHECKING, Callable, Iterator, Protocol, Sequence

if TYPE_CHECKING:
    from .direct_registry import DirectSandboxRegistry

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
    storage_operation_id,
)
from .telemetry import Telemetry
from .runtime_process import RuntimeProcessIdentityError, owned_runtime_process_ticks


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
    @staticmethod
    def _diagnostics_file():
        # Seekable anonymous memory keeps daemonized output semantics without
        # creating two filesystem inodes for every short lifecycle command.
        # These descriptors contain command diagnostics, never durable state.
        try:
            descriptor = os.memfd_create("sandbox-command", os.MFD_CLOEXEC)
        except (AttributeError, OSError):
            return tempfile.TemporaryFile(mode="w+", encoding="utf-8")
        return os.fdopen(descriptor, "w+", encoding="utf-8")

    @staticmethod
    def _wait(process: subprocess.Popen, timeout: float) -> int:
        # Popen.wait(timeout) polls waitpid with exponential sleeps (up to
        # 50 ms). Restores issue several short runsc commands, so these sleeps
        # accumulate even on an idle worker. A pidfd wakes on the exact child
        # exit and does not depend on daemonized children closing stdout.
        try:
            descriptor = os.pidfd_open(process.pid)
        except (AttributeError, OSError):
            return process.wait(timeout=timeout)
        try:
            poller = select.poll()
            poller.register(descriptor, select.POLLIN)
            if not poller.poll(max(0, math.ceil(timeout * 1000))):
                raise subprocess.TimeoutExpired(process.args, timeout)
            return process.wait()
        finally:
            os.close(descriptor)

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
            self._diagnostics_file() as stdout,
            self._diagnostics_file() as stderr,
        ):
            with subprocess.Popen(
                command,
                text=True,
                stdout=stdout,
                stderr=stderr,
            ) as process:
                try:
                    returncode = self._wait(process, timeout)
                except BaseException:
                    process.kill()
                    process.wait()
                    raise
            stdout.seek(0)
            stderr.seek(0)
            rendered_stdout = stdout.read()
            rendered_stderr = stderr.read()
        return CommandResult(
            argv=command,
            returncode=returncode,
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
        if type(pid) is not int or pid <= 1:
            raise DirectWardenError("refusing to fence a system process PID")
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
    application_memory_root: Path | None = None
    reflink_memory_restore: bool = False
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
        if (
            self.application_memory_root is not None
            and not self.application_memory_root.is_absolute()
        ):
            raise ValueError("application_memory_root must be absolute")
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
    workspace_directory: str = ""
    memory: MemoryBackingRef | None = None

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
        if bool(self.workspace_directory) != (self.memory is not None):
            raise ValueError("split backing requires workspace and memory identities")
        if self.workspace_directory and not _SAFE_COMPONENT.fullmatch(
            self.workspace_directory
        ):
            raise ValueError("workspace_directory is invalid")
        if (
            self.memory is not None
            and self.memory.allocation_id != self.memory_directory
        ):
            raise ValueError("memory allocation does not match its directory")

    @property
    def workspace_volume_id(self) -> str:
        """Storage identity; legacy readers share the old memory directory."""
        return self.workspace_directory or self.memory_directory


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
        memory_backing: MemoryBackingStore | None = None,
        memory_capacity: DirectSandboxRegistry | None = None,
    ) -> None:
        self.config = config
        self.runner = runner or SubprocessCommandRunner()
        self.fencer = fencer or LinuxPidfdFencer(proc_root=config.proc_root)
        self.storage = storage
        self.memory_backing = memory_backing
        self.memory_capacity = memory_capacity
        if config.reflink_memory_restore and (memory_backing is None or memory_capacity is None):
            raise ValueError("reflink memory restore requires quota-owned split backing and capacity ledger")
        if memory_backing is not None:
            memory_backing.configure_reflink_restore(config.reflink_memory_restore)
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
            self._require_memory_allocation(sandbox)
            active_memory = self._active_memory_root(sandbox)
            active_memory.mkdir(mode=0o700, parents=True, exist_ok=True)
            self._require_private_directory(active_memory, "active memory directory")
            self._checked(
                *self._common(sandbox),
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
            record = self.workspace_record(sandbox)
            if record.state == StorageVolumeState.PUBLISHED:
                return record
            if record.state in {StorageVolumeState.MOUNTED, StorageVolumeState.SEALED}:
                # Imported checkpoints can be logically parked while metadata
                # repair still owns a writable COW mount. Seal/release under
                # the lifecycle lock before capturing the upload revision.
                self._release_parked_storage(sandbox, operation_seed=operation_id)
                record = self.workspace_record(sandbox)
            revision = record.revision
        # The sealed layers are immutable. Do not hold the Warden lock across
        # remote uploads: local wake/delete can supersede publication. Fence the
        # request so a delayed publisher cannot seal a newly resumed filesystem.
        record = self.storage.ensure_published(
            self._storage_owner(sandbox),
            operation_id=operation_id,
            expected_revision=revision,
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
                and self._sentry_identity_matches(
                    sandbox,
                    record.sentry_pid,
                    record.sentry_start_time_ticks,
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
            self._prepare_restore_memory(sandbox)
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
            self._require_memory_allocation(sandbox)
            if running.sentry_pid is None or running.sentry_start_time_ticks is None:
                raise DirectWardenError("running journal lacks a sentry identity")
            handle = self._open_sentry_fence(
                sandbox,
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
                            *self._common(sandbox),
                            "checkpoint",
                            "--hibernate",
                            f"--image-path={generation}",
                            sandbox.container_id,
                        )
                    if not handle.alive():
                        raise DirectWardenError(
                            "sentry exited before its capture was durably published"
                        )
                    if sandbox.memory is not None:
                        pid, ticks, status = self._state_identity_status(sandbox)
                        if (pid, ticks, status) != (
                            running.sentry_pid,
                            running.sentry_start_time_ticks,
                            "paused",
                        ):
                            raise DirectWardenError(
                                "capture barrier does not own the paused original sentry"
                            )
                        storage_record = self.workspace_record(sandbox)
                        self.storage.prepare_capture(
                            self._storage_owner(sandbox),
                            operation_id=storage_operation_id(
                                self._storage_owner(sandbox), operation_id, "workspace-prepare"
                            ),
                            expected_revision=storage_record.revision,
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
                    self._delete_runtime(sandbox)
                # runsc delete removes its filestore from the merged rootfs.
                # Do this before detaching the overlay so that the sealed
                # layer contains the final, cleaned-up filesystem state.
                with self.telemetry.span("sandbox.park.release_storage"):
                    self._release_parked_storage(
                        sandbox, operation_seed=operation_id, manifest=manifest
                    )
                with self.telemetry.span("sandbox.park.commit_journal"):
                    parked = journal.commit_parked(
                        manifest,
                        operation_id=operation_id,
                        expected_revision=pending.revision,
                    )
                    self._prepare_restore_memory(sandbox)
                    return parked
            finally:
                handle.close()

    def prepare_restore_memory(self, sandbox: DirectSandbox) -> None:
        """Select parked placement before service admission quotes its cost.

        This does not launch a candidate or grant execution authority. Repeating
        it after import/recovery is safe; a live owner can never change roots.
        """
        if not self.config.reflink_memory_restore:
            return
        with self._locked(sandbox):
            self._require_state(sandbox, HibernationState.PARKED)
            self._prepare_restore_memory(sandbox)

    def _prepare_restore_memory(self, sandbox: DirectSandbox) -> None:
        if not self.config.reflink_memory_restore:
            return
        if sandbox.memory is None or self.memory_backing is None:
            raise DirectWardenError("reflink restore requires an owned memory allocation")
        self.memory_backing.prepare_file_restore(
            sandbox.memory, sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
        )

    def flush_reclaimable_memory(self, sandbox: DirectSandbox) -> bool:
        """Write dirty application pages without checkpointing the runtime.

        The owned descriptor remains valid through concurrent lifecycle work;
        never hold the lifecycle lock over storage I/O. The caller must recheck
        its wait/cancellation and measured pressure before reclaiming pages.
        """
        if not self.config.reflink_memory_restore or sandbox.memory is None:
            return False
        if self.memory_backing is None:
            raise DirectWardenError("split memory allocator is unavailable")
        with self.memory_backing.read_lease(
            sandbox.memory, sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
        ):
            return self._flush_reclaimable_memory(sandbox)

    def _flush_reclaimable_memory(self, sandbox: DirectSandbox) -> bool:
        with self._locked(sandbox):
            before = self._journal(sandbox).load()
            if before is None or before.state != HibernationState.RUNNING:
                return False
            self._require_memory_allocation(sandbox)
            if self.application_memory_mode(sandbox.sandbox_id, sandbox.sandbox_generation) != "file":
                return False
            path = self._active_memory_root(sandbox) / _ACTIVE_APPLICATION_MEMORY
            fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077):
                os.close(fd)
                raise DirectWardenError("application memory file is not privately owned")
        try:
            with self.telemetry.span("sandbox.memory.flush_for_reclaim"):
                os.fdatasync(fd)
            with self._locked(sandbox):
                after = self._journal(sandbox).load()
                if after != before:
                    return False
                try:
                    current = path.lstat()
                except FileNotFoundError:
                    return False
                return (current.st_dev, current.st_ino) == (info.st_dev, info.st_ino)
        finally:
            os.close(fd)

    def resume(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
        timings: dict[str, float] | None = None,
        before_restore: Callable[[], object] | None = None,
    ) -> HibernationRecord:
        timings = timings if timings is not None else {}
        resume_started = time.monotonic()
        with self._locked(sandbox):
            phase = time.monotonic()
            journal = self._journal(sandbox)
            parked = self._require_state(sandbox, HibernationState.PARKED)
            try:
                self.ensure_workspace_mounted(
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
                self._require_checkpoint_components(sandbox, manifest)
                if sandbox.memory is not None:
                    self._remove_captured_filestore(sandbox)
                # Storage-native resume mounts a new destination-local view above.
                # Bind that exact rootfs ledger to the checkpoint before runsc is
                # allowed to construct or resume any workload task.
                self._require_managed_process_ledger(sandbox, manifest)
                # Network preparation can overlap storage mounting/validation,
                # but must succeed before a restore candidate can be started.
                if before_restore is not None:
                    before_restore()
                self._prepare_restore_memory(sandbox)
                self._retain_restore_source(sandbox, manifest)
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
                # Keep the OCI CPU quota during boot. The optional startup
                # burst removes it before runsc sizes the sentry's Go scheduler,
                # letting every concurrent restore use all host CPUs.
                self._checked(
                    *self._common(sandbox),
                    "restore",
                    "--detach",
                    "--background",
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
                candidate = self._open_sentry_fence(sandbox, pid, ticks)
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
                self._ensure_runtime_running(
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
                self._finalize_restore_artifacts(sandbox, manifest)
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
                durable is not None
                and durable.state == HibernationState.RECOVERY_REQUIRED
            ):
                # Quarantine is already durable. Its volume may deliberately
                # remain unreadable until operator recovery or deletion.
                return durable
            if (
                durable is not None
                and self.workspace_record(sandbox).state == StorageVolumeState.ERROR
            ):
                return self._quarantine_storage_error(sandbox, journal, durable)
            if (
                durable is not None
                and durable.state != HibernationState.RUNNING
                and sandbox.memory is None
            ):
                self.ensure_workspace_mounted(
                    sandbox,
                    operation_id=f"reconcile:{durable.revision}:storage-mount",
                )
                self.rootfs_lifecycle.resume_sandbox(sandbox)
            if durable is not None and durable.state == HibernationState.RESTORING:
                return self._reconcile_restoring(sandbox, journal, durable)
            if durable is not None:
                for pid, ticks in (
                    (durable.sentry_pid, durable.sentry_start_time_ticks),
                    (durable.candidate_pid, durable.candidate_start_time_ticks),
                ):
                    self._sentry_identity_matches(sandbox, pid, ticks)
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
                handle = self._open_sentry_fence(
                    sandbox,
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
                self._delete_runtime(sandbox)
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
                self._prepare_restore_memory(sandbox)
                return parked

            if result.action == HibernationRecoveryAction.RESUME_OR_RETRY_HIBERNATE:
                if record.sentry_pid is None or record.sentry_start_time_ticks is None:
                    raise DirectWardenError(
                        "interrupted capture has no live sentry identity"
                    )
                handle = self._open_sentry_fence(
                    sandbox,
                    record.sentry_pid,
                    record.sentry_start_time_ticks,
                )
                try:
                    self._abort_workspace_capture(
                        sandbox, operation_seed=record.operation_id
                    )
                    self._ensure_runtime_running(
                        sandbox,
                        expected_pid=record.sentry_pid,
                        expected_start_time_ticks=record.sentry_start_time_ticks,
                    )
                    pid, ticks = record.sentry_pid, record.sentry_start_time_ticks
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
                self._prepare_restore_memory(sandbox)
            return record

    def _quarantine_storage_error(
        self,
        sandbox: DirectSandbox,
        journal: HibernationJournal,
        record: HibernationRecord,
    ) -> HibernationRecord:
        """Fence this incarnation without remounting an unusable volume."""
        identities = {
            (pid, ticks)
            for pid, ticks in (
                (record.sentry_pid, record.sentry_start_time_ticks),
                (record.candidate_pid, record.candidate_start_time_ticks),
            )
            if pid is not None and ticks is not None
        }
        if (
            record.state == HibernationState.RESTORING
            and record.authority == HibernationAuthority.PARKED
        ):
            # Restore may have daemonized before recording its candidate.
            candidate = self._candidate_identity_or_none(sandbox)
            if candidate is not None:
                identities.add(candidate)
        for pid, ticks in identities:
            # A stale PID from before reboot must never fence its new owner.
            if not hibernation_process_identity_matches(
                pid, ticks, proc_root=self.config.proc_root
            ):
                continue
            handle = self._open_sentry_fence(sandbox, pid, ticks)
            try:
                handle.terminate(timeout=self.config.stop_timeout_seconds)
                if handle.alive():
                    raise DirectWardenError("storage-error runtime could not be fenced")
            finally:
                handle.close()
        return journal.quarantine(
            reason="storage-native volume is in error state",
            expected_revision=record.revision,
            live_process_confirmed_dead=True,
        )

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
            and self._sentry_identity_matches(
                sandbox,
                restoring.candidate_pid,
                restoring.candidate_start_time_ticks,
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

        candidate = self._open_sentry_fence(sandbox, *candidate_identity)
        try:
            self._ensure_runtime_running(
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
                self._finalize_restore_artifacts(sandbox, manifest)
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
        if (
            snapshot is not None
            and snapshot.state
            in {
                HibernationState.HIBERNATING,
                HibernationState.RESTORING,
            }
            and snapshot.authority != HibernationAuthority.PENDING
        ):
            self.reconcile(sandbox)
        with self._locked(sandbox):
            journal = self._journal(sandbox)
            record = journal.load()
            if record is None:
                self._parked_manifest_path(sandbox).unlink(missing_ok=True)
                return
            reaped_capture = (
                record.state == HibernationState.HIBERNATING
                and record.authority == HibernationAuthority.PENDING
            )
            if reaped_capture:
                # A release failure can follow successful capture and runtime
                # teardown. Deletion must not remount the failed volume just
                # to reconcile a checkpoint the caller has asked to discard.
                if (
                    record.sentry_pid is not None
                    or record.candidate_pid is not None
                    or self._candidate_identity_or_none(sandbox) is not None
                ):
                    raise DirectWardenError(
                        "reaped capture still has a runtime identity"
                    )
                self._best_effort_delete(sandbox)
            if (
                record.state
                not in {
                    HibernationState.RUNNING,
                    HibernationState.PARKED,
                    HibernationState.RECOVERY_REQUIRED,
                }
                and not reaped_capture
            ):
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
                if self._sentry_identity_matches(sandbox, pid, ticks):
                    try:
                        handle = self._open_cleanup_fence(
                            sandbox, pid, "sandbox", expected_ticks=ticks
                        )
                    except ProcessLookupError:
                        pass
                    else:
                        try:
                            handle.terminate(timeout=self.config.stop_timeout_seconds)
                        finally:
                            handle.close()
            self._delete_runtime(sandbox)

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
        handle: ProcessHandle,
    ) -> None:
        if not handle.alive():
            journal.quarantine(
                reason="sentry died before hibernation publication",
                expected_revision=hibernating.revision,
                live_process_confirmed_dead=True,
            )
            return
        self._abort_workspace_capture(sandbox, operation_seed=hibernating.operation_id)
        # A checkpoint RPC can pause the original before the export file is
        # created. Artifact presence cannot prove whether execution is paused.
        self._ensure_runtime_running(
            sandbox,
            expected_pid=hibernating.sentry_pid,
            expected_start_time_ticks=hibernating.sentry_start_time_ticks,
        )
        pid, ticks = hibernating.sentry_pid, hibernating.sentry_start_time_ticks
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
                    candidate = self._open_sentry_fence(sandbox, *identity)
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
            parked = journal.rollback_restore(
                operation_id=restoring.operation_id,
                expected_revision=current.revision,
                candidate_reaped=True,
            )
            self._prepare_restore_memory(sandbox)
            return parked
        finally:
            if opened_candidate and candidate is not None:
                candidate.close()

    def _ensure_runtime_running(
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
                raise DirectWardenError("runtime identity changed before resume")
        if status == "paused":
            self._checked(
                *self._state_prefix(),
                "resume",
                sandbox.container_id,
            )
            pid, ticks, status = self._state_identity_status(sandbox)
            if (pid, ticks) != (expected_pid, expected_start_time_ticks):
                raise DirectWardenError("runtime identity changed while resuming")
        if status != "running":
            raise DirectWardenError(f"runtime did not become running: {status}")

    def _finalize_restore_artifacts(
        self,
        sandbox: DirectSandbox,
        manifest: HibernationManifest,
    ) -> None:
        with self.telemetry.span("sandbox.restore.artifact_unlink"):
            self.artifacts.delete_published(
                manifest,
                allow_consumed_main_memory=True,
            )
        if not self.config.reflink_memory_restore:
            self._release_retired_memory_capacity(sandbox)

    def _retain_restore_source(
        self, sandbox: DirectSandbox, manifest: HibernationManifest,
    ) -> None:
        if not self.config.reflink_memory_restore:
            return
        if self.memory_capacity is None or self.memory_backing is None or sandbox.memory is None:
            raise DirectWardenError("reflink restore capacity ownership is unavailable")
        source = self.artifacts.generation_path(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            hibernation_generation=manifest.hibernation_generation,
        ) / _APPLICATION_MEMORY
        info = source.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
            raise DirectWardenError("restore source is not an owned regular file")
        # XFS enforces project quotas in filesystem blocks. Charge that exact
        # rounded allocation, never the sparse logical heap length.
        allocated = max(4096, ((info.st_blocks * 512 + 4095) // 4096) * 4096)
        self.memory_capacity.reserve_reflink_overlap(
            sandbox.sandbox_id, sandbox.sandbox_generation,
            manifest.hibernation_generation, allocated,
            manifest_sha256=manifest.metadata_sha256,
        )
        self.memory_backing.retain_checkpoint(
            sandbox.memory, sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            hibernation_generation=manifest.hibernation_generation,
            manifest_sha256=manifest.metadata_sha256, allocated_bytes=allocated,
        )

    def _release_retired_memory_capacity(self, sandbox: DirectSandbox) -> None:
        if self.memory_capacity is None or sandbox.memory is None:
            return
        if self.memory_backing is None:
            raise DirectWardenError("split memory allocator is unavailable")
        for claim in self.memory_capacity.list_reflink_overlaps(
            sandbox.sandbox_id, sandbox.sandbox_generation,
        ):
            if self.memory_backing.release_retained_checkpoint(
                sandbox.memory,
                hibernation_generation=claim.hibernation_generation,
                manifest_sha256=claim.manifest_sha256,
            ):
                self.memory_capacity.release_reflink_overlap(
                    sandbox.sandbox_id, sandbox.sandbox_generation,
                    claim.hibernation_generation,
                    manifest_sha256=claim.manifest_sha256,
                )

    def release_deleted_memory_capacity(self, sandbox: DirectSandbox) -> None:
        """Finish capacity cleanup after the allocator's successful deletion."""
        with self._locked(sandbox):
            if self._journal(sandbox).load() is not None:
                raise DirectWardenError("memory deletion still has lifecycle authority")
            if sandbox.memory is not None and self.memory_backing is not None:
                if os.path.lexists(self.memory_backing.root / sandbox.memory.allocation_id):
                    raise DirectWardenError("memory allocation still exists")
                self._release_retired_memory_capacity(sandbox)

    def reconcile_retired_memory_capacity(self) -> int:
        """Use durable overlap claims as the existing maintenance worklist."""
        if not self.config.reflink_memory_restore:
            return 0
        if self.memory_capacity is None or self.memory_backing is None:
            raise DirectWardenError("reflink restore capacity ownership is unavailable")
        claims = self.memory_capacity.list_reflink_overlaps()
        checkpoints = {}
        registrations = {}
        for claim in claims:
            owner = (claim.sandbox_id, claim.sandbox_generation)
            if owner not in registrations:
                registrations[owner] = self.memory_capacity.get(claim.sandbox_id)
            registration = registrations[owner]
            if (registration is None or registration.sandbox_generation != claim.sandbox_generation
                    or registration.memory_reference is None):
                raise DirectWardenError("retained checkpoint has no matching registry owner")
            checkpoint = RetainedCheckpointRef(registration.memory_reference,
                                               claim.hibernation_generation, claim.manifest_sha256)
            checkpoints[checkpoint] = claim

        def release_claim(checkpoint):
            claim = checkpoints[checkpoint]
            self.memory_capacity.release_reflink_overlap(
                claim.sandbox_id, claim.sandbox_generation, claim.hibernation_generation,
                manifest_sha256=claim.manifest_sha256,
            )

        if not checkpoints:
            return 0
        with self.telemetry.span("sandbox.memory.retire_physical",
                                 attributes={"memory.retirement.candidates": len(checkpoints)}) as span:
            released = self.memory_backing.release_retained_checkpoints(
                checkpoints, release_claim=release_claim)
            span.set_attribute("memory.retirement.released", released)
            return released

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
                MemoryBackingStore.MARKER,
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
            with self.telemetry.span("sandbox.restore.artifact_unlink"):
                self.artifacts.delete_published(
                    manifest,
                    allow_consumed_main_memory=True,
                )
        # Artifact removal may have committed before a process crash, leaving
        # only the capacity/project journals to finish. Inventory alone misses it.
        if not self.config.reflink_memory_restore:
            self._release_retired_memory_capacity(sandbox)

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
        workspace = None
        if sandbox.memory is not None:
            storage_record = self.workspace_record(sandbox)
            if storage_record.state != StorageVolumeState.CAPTURE_PREPARED:
                raise DirectWardenError("workspace capture is not prepared")
            workspace = WorkspaceCaptureRef(
                storage_record.volume_id, storage_record.capture_id
            )
        return HibernationManifest(
            version=3 if sandbox.memory is not None else 2,
            workspace=workspace,
            memory=sandbox.memory,
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

    def _sentry_identity(
        self,
        sandbox: DirectSandbox,
        pid: int,
        expected_ticks: int | None = None,
    ) -> int:
        try:
            boot = self._current_process_boot(sandbox)
            marker = self._process_boot_marker(sandbox)
            ticks = owned_runtime_process_ticks(
                pid,
                proc_root=self.config.proc_root,
                runsc=self.config.runsc,
                runtime_root=self.config.runtime_root,
                bundle=sandbox.bundle,
                container_id=sandbox.container_id,
                expected_ticks=expected_ticks,
            )
            if not marker.exists():
                marker.parent.mkdir(mode=0o700, exist_ok=True)
                with marker.open("x") as stream:
                    stream.write(boot + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                marker.chmod(0o600)
                directory = os.open(marker.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            return ticks
        except ProcessLookupError:
            raise
        except (RuntimeProcessIdentityError, OSError, ValueError) as exc:
            raise DirectWardenError(str(exc)) from exc

    def _sentry_identity_matches(
        self,
        sandbox: DirectSandbox,
        pid: int | None,
        ticks: int | None,
    ) -> bool:
        if not hibernation_process_identity_matches(
            pid, ticks, proc_root=self.config.proc_root
        ):
            return False
        try:
            self._sentry_identity(sandbox, pid, ticks)
        except ProcessLookupError:
            return False
        return True

    def _current_process_boot(self, sandbox: DirectSandbox) -> str:
        boot = str(
            UUID(
                (self.config.proc_root / "sys/kernel/random/boot_id")
                .read_text()
                .strip()
            )
        )
        marker = self._process_boot_marker(sandbox)
        if marker.exists() and marker.read_text().strip() != boot:
            raise RuntimeProcessIdentityError(
                "runtime identity belongs to another boot"
            )
        return boot

    def _process_boot_marker(self, sandbox: DirectSandbox) -> Path:
        return self.config.runtime_root / "warden-process-owners" / sandbox.container_id

    def _open_sentry_fence(
        self,
        sandbox: DirectSandbox,
        pid: int,
        ticks: int,
    ) -> ProcessHandle:
        self._sentry_identity(sandbox, pid, ticks)
        handle = self.fencer.open(pid, ticks)
        try:
            self._sentry_identity(sandbox, pid, ticks)
        except BaseException:
            handle.close()
            raise
        return handle

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
            ticks = self._sentry_identity(sandbox, pid)
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
                if inventory is None:
                    # runsc marshals an empty container slice as JSON null.
                    inventory = []
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
            ticks = self._sentry_identity(sandbox, pid)
        except ProcessLookupError:
            return None
        except ValueError as exc:
            raise DirectWardenError("cannot read sentry process identity") from exc
        return pid, ticks

    def application_memory_mode(self, sandbox_id: str, generation: int) -> str:
        """Owner-local placement evidence; this does not grant lifecycle authority."""
        if self.memory_backing is not None:
            mode = self.memory_backing.active_mode(sandbox_id, generation)
            if mode is not None:
                return mode
        # Legacy workers have one fixed layout. A not-yet-prepared allocation
        # on a RAM-capable worker must retain conservative RAM admission.
        return "ram" if self.config.application_memory_root is not None else "file"

    def _common(self, sandbox: DirectSandbox) -> tuple[str, ...]:
        ram = self.application_memory_mode(sandbox.sandbox_id, sandbox.sandbox_generation) == "ram"
        root = self.config.application_memory_root if ram else self.config.memory_root
        if root is None:
            raise DirectWardenError("RAM memory placement has no configured backing root")
        return (
            str(self.config.runsc),
            f"--root={self.config.runtime_root}",
            "--platform=systrap",
            f"--network={self.config.network}",
            f"--application-memory-file-dir={root}",
            *(
                ("--application-memory-ram-backing=true",)
                if ram
                else ()
            ),
            *(("--application-memory-reflink-restore=true",)
              if self.config.reflink_memory_restore and not ram else ()),
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
        self._delete_runtime(sandbox, checked=False)

    def _open_cleanup_fence(
        self,
        sandbox: DirectSandbox,
        pid: int,
        role: str,
        expected_ticks: int | None = None,
    ) -> ProcessHandle:
        # Pin first, but never signal until provenance has been established.
        # A dying gofer can lose /proc/PID/exe before its stat becomes Z; the
        # pidfd lets that exit count as completed cleanup without trusting it.
        ticks = linux_process_start_time_ticks(pid, proc_root=self.config.proc_root)
        if expected_ticks is not None and ticks != expected_ticks:
            raise DirectWardenError("cleanup process identity changed")
        try:
            handle = self.fencer.open(pid, ticks)
        except DirectWardenError:
            # Exit between stat and pidfd_open is normal. A replacement owner
            # still fails provenance/start-time validation and is never killed.
            owned_runtime_process_ticks(
                pid,
                proc_root=self.config.proc_root,
                runsc=self.config.runsc,
                runtime_root=self.config.runtime_root,
                bundle=sandbox.bundle,
                container_id=sandbox.container_id,
                role=role,
                expected_ticks=ticks,
            )
            raise
        try:
            if role == "sandbox":
                self._sentry_identity(sandbox, pid, ticks)
            else:
                self._current_process_boot(sandbox)
                owned_runtime_process_ticks(
                    pid,
                    proc_root=self.config.proc_root,
                    runsc=self.config.runsc,
                    runtime_root=self.config.runtime_root,
                    bundle=sandbox.bundle,
                    container_id=sandbox.container_id,
                    role=role,
                    expected_ticks=ticks,
                )
        except Exception as exc:
            exited = not handle.alive()
            if not exited and isinstance(handle, LinuxPidfdHandle):
                poller = select.poll()
                poller.register(handle.pidfd, select.POLLIN)
                exited = bool(
                    poller.poll(max(1, int(self.config.stop_timeout_seconds * 1000)))
                )
            handle.close()
            if exited:
                raise ProcessLookupError(pid) from exc
            raise
        return handle

    def _fence_delete_metadata(self, sandbox: DirectSandbox) -> None:
        """Reap verified owners and clear numeric PID fields before runsc cleanup.

        runsc's metadata lock serializes this edit with runsc state updates.
        Leaving numeric PIDs for downstream SIGKILL would reopen PID reuse races,
        including after our sentry pidfd has already reported process exit.
        """
        stem = f"{sandbox.container_id}_sandbox:{sandbox.container_id}"
        path = self.config.runtime_root / (stem + ".state")
        lock_fd = os.open(
            self.config.runtime_root / (stem + ".lock"),
            os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW,
            0o600,
        )
        handles: list[ProcessHandle] = []
        try:
            deadline = time.monotonic() + self.config.command_timeout_seconds
            while True:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise DirectWardenError(
                            "timed out acquiring runsc cleanup metadata lock"
                        )
                    time.sleep(0.01)
            try:
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            except FileNotFoundError:
                return
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.geteuid()
                    or info.st_mode & 0o022
                ):
                    raise DirectWardenError("runsc cleanup metadata is not private")
                raw = stream.read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise DirectWardenError("runsc cleanup metadata exceeds its bound")
            state = json.loads(raw)
            if not isinstance(state, dict):
                raise DirectWardenError("runsc cleanup metadata is not an object")
            runtime = state.get("sandbox")
            if state.get("id") != sandbox.container_id or (
                runtime is not None
                and (
                    not isinstance(runtime, dict)
                    or runtime.get("id") != sandbox.container_id
                )
            ):
                raise DirectWardenError("runsc cleanup metadata has another owner")
            for role, pid in (
                ("sandbox", (runtime or {}).get("pid", 0)),
                ("gofer", state.get("goferPid", 0)),
            ):
                if type(pid) is not int or pid < 0 or pid == 1:
                    raise DirectWardenError("runsc cleanup metadata has an unsafe PID")
                if not pid:
                    continue
                try:
                    handle = self._open_cleanup_fence(sandbox, pid, role)
                    handles.append(handle)
                except ProcessLookupError:
                    # Missing/zombie processes need no signal. Their stored PIDs
                    # must still be cleared before the numeric-PID teardown.
                    continue
            # Verify every target before sending even the first signal.
            for handle in handles:
                handle.terminate(timeout=self.config.stop_timeout_seconds)
            state["goferPid"] = 0
            if runtime is not None:
                runtime["pid"] = 0
            descriptor, temporary = tempfile.mkstemp(
                prefix=".warden-delete-", dir=path.parent
            )
            try:
                with os.fdopen(descriptor, "w") as stream:
                    json.dump(state, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, path)
                directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
        except (ValueError, RuntimeProcessIdentityError) as exc:
            raise DirectWardenError(
                f"cannot prove runsc cleanup process ownership: {exc}"
            ) from exc
        finally:
            for handle in handles:
                handle.close()
            os.close(lock_fd)

    def _delete_runtime(self, sandbox: DirectSandbox, *, checked: bool = True) -> None:
        self._fence_delete_metadata(sandbox)
        result = self.runner.run(
            (*self._state_prefix(), "delete", "--force", sandbox.container_id),
            timeout=self.config.command_timeout_seconds,
        )
        if result.returncode == 0:
            self._process_boot_marker(sandbox).unlink(missing_ok=True)
        elif checked:
            raise DirectWardenError(f"runsc cleanup failed: {result.stderr}")

    def workspace_record(self, sandbox: DirectSandbox) -> StorageVolumeRecord:
        """Read the workspace bound to this incarnation, checking owner and path.

        This is storage evidence, not a grant to execute or change lifecycle.
        It covers both legacy coupled volumes and split workspace volumes.
        """
        record = self.storage.get_volume(sandbox.workspace_volume_id)
        self._validate_storage_record(sandbox, record)
        return record

    def storage_records_snapshot(
        self,
        sandboxes: Sequence[DirectSandbox],
    ) -> dict[str, StorageVolumeRecord]:
        """Resolve sandbox storage ownership with at most one daemon RPC."""

        expected: dict[str, DirectSandbox] = {}
        for sandbox in sandboxes:
            if sandbox.workspace_volume_id in expected:
                raise DirectWardenError(
                    "direct registry contains duplicate storage-native ownership"
                )
            expected[sandbox.workspace_volume_id] = sandbox
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
        ) != self.config.memory_root / sandbox.workspace_volume_id:
            raise DirectWardenError(
                "storage-native volume does not own this sandbox incarnation"
            )

    @staticmethod
    def _storage_owner(sandbox: DirectSandbox) -> StorageVolumeOwner:
        return StorageVolumeOwner(
            volume_id=sandbox.workspace_volume_id,
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
        )

    def ensure_workspace_mounted(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> StorageVolumeRecord:
        """Prepare this incarnation's workspace lease without granting execution.

        Import and restore share this idempotent preparation operation. The
        caller retains its lifecycle fence; rootfs preparation and execution
        handoff remain separate steps owned by the existing journal.
        """
        record = self.storage.ensure_mounted(
            self._storage_owner(sandbox),
            operation_id=operation_id,
        )
        self._validate_storage_record(sandbox, record)
        return record

    def _require_memory_allocation(self, sandbox: DirectSandbox) -> None:
        if sandbox.memory is None:
            return
        if self.memory_backing is None:
            raise DirectWardenError("split memory backing is not configured")
        self.memory_backing.require(
            sandbox.memory,
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
        )

    def _require_checkpoint_components(
        self, sandbox: DirectSandbox, manifest: HibernationManifest
    ) -> None:
        self._require_memory_allocation(sandbox)
        if sandbox.memory is None:
            if manifest.version != 2:
                raise DirectWardenError(
                    "split checkpoint cannot use a legacy allocation"
                )
            return
        record = self.workspace_record(sandbox)
        expected = WorkspaceCaptureRef(record.volume_id, record.capture_id)
        if (
            manifest.version != 3
            or manifest.memory != sandbox.memory
            or manifest.workspace != expected
        ):
            raise DirectWardenError(
                "checkpoint component ownership differs from its commit"
            )

    def _abort_workspace_capture(
        self, sandbox: DirectSandbox, *, operation_seed: str
    ) -> None:
        if sandbox.memory is None:
            return
        record = self.workspace_record(sandbox)
        if record.state == StorageVolumeState.CAPTURE_PREPARED:
            self.storage.abort_capture(
                self._storage_owner(sandbox),
                operation_id=storage_operation_id(
                    self._storage_owner(sandbox), operation_seed, "workspace-abort"
                ),
                expected_revision=record.revision,
            )
        elif record.state != StorageVolumeState.MOUNTED:
            raise DirectWardenError("workspace capture has no safe abort decision")

    @staticmethod
    def _remove_captured_filestore(sandbox: DirectSandbox) -> None:
        # Only the imported writable clone is changed. Its immutable captured
        # filestore remains part of the workspace commit for crash recovery.
        path = sandbox.bundle / "rootfs" / f".gvisor.filestore.{sandbox.container_id}"
        try:
            info = path.lstat()
        except FileNotFoundError:
            return
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
            raise DirectWardenError(
                "captured runtime filestore is not an owned regular file"
            )
        path.unlink()
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _release_parked_storage(
        self,
        sandbox: DirectSandbox,
        *,
        operation_seed: str,
        manifest: HibernationManifest | None = None,
    ) -> None:
        record = self.workspace_record(sandbox)
        if sandbox.memory is not None:
            if manifest is None:
                durable = self._journal(sandbox).load()
                if durable is None:
                    raise DirectWardenError("split capture has no lifecycle authority")
                manifest = self.artifacts.load_complete(
                    sandbox_id=sandbox.sandbox_id,
                    sandbox_generation=sandbox.sandbox_generation,
                    hibernation_generation=durable.hibernation_generation,
                )
            self._require_checkpoint_components(sandbox, manifest)
            if record.state == StorageVolumeState.CAPTURE_PREPARED:
                self.rootfs_lifecycle.park_sandbox(sandbox)
                record = self.storage.commit_capture(
                    self._storage_owner(sandbox),
                    operation_id=storage_operation_id(
                        self._storage_owner(sandbox), operation_seed, "workspace-commit"
                    ),
                    expected_revision=record.revision,
                )
            elif record.state == StorageVolumeState.MOUNTED:
                # Import validation/failed restores own only an uncommitted COW.
                self._rollback_parked_storage_mount(
                    sandbox, operation_seed=operation_seed
                )
                return
        elif record.state == StorageVolumeState.MOUNTED:
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

        record = self.workspace_record(sandbox)
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
        ram = self.application_memory_mode(sandbox.sandbox_id, sandbox.sandbox_generation) == "ram"
        root = self.config.application_memory_root if ram else self.config.memory_root
        if root is None:
            raise DirectWardenError("RAM memory placement has no configured backing root")
        path = root / sandbox.memory_directory
        if path.parent != root:
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
