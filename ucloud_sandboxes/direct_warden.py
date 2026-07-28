from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
import fcntl
import json
import logging
import os
from pathlib import Path
import re
import select
import signal
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
    ) -> None:
        self.config = config
        self.runner = runner or SubprocessCommandRunner()
        self.fencer = fencer or LinuxPidfdFencer(proc_root=config.proc_root)
        self.journals = HibernationJournalStore(config.journal_root)
        self.artifacts = HibernationArtifactStore(config.artifact_root)
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
            if (
                not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key)
                or "\0" in value
            ):
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
            timings["validate_artifact"] = (time.monotonic() - phase) * 1000
            phase = time.monotonic()
            restoring = journal.begin_restore(
                operation_id=operation_id,
                expected_revision=parked.revision,
            )
            timings["begin_restore_journal"] = (time.monotonic() - phase) * 1000
            candidate: ProcessHandle | None = None
            restore_started = False
            candidate_record: HibernationRecord | None = None
            try:
                restore_flags = ["--detach"]
                if self.config.restore_background:
                    restore_flags.append("--background")
                if self.config.restore_cpu_startup_burst:
                    restore_flags.append("--cpu-startup-burst")
                phase = time.monotonic()
                self._checked(
                    *self._common(),
                    "restore",
                    *restore_flags,
                    f"--image-path={self.artifacts.generation_path(sandbox_id=sandbox.sandbox_id, sandbox_generation=sandbox.sandbox_generation, hibernation_generation=parked.hibernation_generation)}",
                    f"--bundle={sandbox.bundle}",
                    sandbox.container_id,
                )
                timings["runsc_restore"] = (time.monotonic() - phase) * 1000
                restore_started = True
                phase = time.monotonic()
                pid, ticks = self._state_identity(sandbox)
                timings["runsc_state"] = (time.monotonic() - phase) * 1000
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
                self._checked(
                    *self._state_prefix(),
                    "exec",
                    sandbox.container_id,
                    *self.config.readiness_command,
                )
                timings["readiness_exec"] = (time.monotonic() - phase) * 1000
            except Exception:
                self._rollback_restore(
                    sandbox,
                    journal=journal,
                    restoring=restoring,
                    manifest=manifest,
                    candidate=candidate,
                    restore_started=restore_started,
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
            # The new backend now owns the canonical memory inode. Cleanup is
            # ancillary after RUNNING commits: final delete retries failures,
            # rather than reporting a failed wake while a live backend exists.
            phase = time.monotonic()
            try:
                self.artifacts.delete_published(
                    manifest,
                    allow_consumed_main_memory=True,
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
                return journal.commit_parked(
                    manifest,
                    operation_id=record.operation_id,
                    expected_revision=pending.revision,
                )

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
                return journal.rollback_restore(
                    operation_id=record.operation_id,
                    expected_revision=record.revision,
                )

            if result.action == HibernationRecoveryAction.VERIFY_CANDIDATE:
                if (
                    record.candidate_pid is None
                    or record.candidate_start_time_ticks is None
                ):
                    raise DirectWardenError("restore candidate has no process identity")
                manifest = self.artifacts.load_published_metadata(
                    sandbox_id=sandbox.sandbox_id,
                    sandbox_generation=sandbox.sandbox_generation,
                    hibernation_generation=record.hibernation_generation,
                )
                candidate = self.fencer.open(
                    record.candidate_pid,
                    record.candidate_start_time_ticks,
                )
                try:
                    self._checked(
                        *self._state_prefix(),
                        "exec",
                        sandbox.container_id,
                        *self.config.readiness_command,
                    )
                    return journal.commit_running(
                        operation_id=record.operation_id,
                        expected_revision=record.revision,
                        sentry_pid=record.candidate_pid,
                        sentry_start_time_ticks=record.candidate_start_time_ticks,
                    )
                except Exception:
                    self._rollback_restore(
                        sandbox,
                        journal=journal,
                        restoring=record,
                        manifest=manifest,
                        candidate=candidate,
                        restore_started=True,
                    )
                    raise
                finally:
                    candidate.close()

            return record

    def delete(self, sandbox: DirectSandbox) -> None:
        """Delete one settled backend and all of its owned local generations."""
        with self._locked(sandbox):
            journal = self._journal(sandbox)
            record = journal.load()
            if record is None:
                return
            # Deletion can be replayed after a crash that already reaped the
            # sentry but did not remove the journal. Reconcile the durable
            # authority first so a dead exact PID becomes recovery-owned
            # cleanup rather than an unrecoverable fencing error.
            record = HibernationReconciler(
                journal,
                self.artifacts,
                runtime_sha256=self._runtime_fingerprint(sandbox).digest,
                proc_root=self.config.proc_root,
                candidate_identity_resolver=lambda _record: (
                    self._candidate_identity_or_none(sandbox)
                ),
            ).reconcile().record
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

            unified_storage = (
                self.config.memory_root == self.config.artifact_root
            )
            for item in self.artifacts.inventory_incarnation(
                sandbox_id=sandbox.sandbox_id,
                sandbox_generation=sandbox.sandbox_generation,
                ignored_entries=(
                    (
                        "upper",
                        "work",
                        _APPLICATION_MEMORY,
                        _ACTIVE_APPLICATION_MEMORY,
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
                        or item.hibernation_generation
                        != record.hibernation_generation
                    ),
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
    ) -> None:
        generation = self.artifacts.generation_path(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
            hibernation_generation=restoring.hibernation_generation,
        )
        active_memory = self._active_memory_root(sandbox) / _APPLICATION_MEMORY
        if not restore_started:
            journal.rollback_restore(
                operation_id=restoring.operation_id,
                expected_revision=restoring.revision,
            )
            return
        if candidate is None:
            try:
                pid, ticks = self._state_identity(sandbox)
                candidate = self.fencer.open(pid, ticks)
            except Exception as exc:
                raise DirectWardenError(
                    "cannot prove the restore candidate is fenced"
                ) from exc
        try:
            if candidate is not None and candidate.alive():
                candidate.terminate(timeout=self.config.stop_timeout_seconds)
            if (
                active_memory.exists()
                and not (generation / _APPLICATION_MEMORY).exists()
            ):
                self.artifacts.return_consumed_file(
                    manifest,
                    active_root=active_memory.parent,
                    file_name=active_memory.name,
                )
            self._best_effort_delete(sandbox)
            current = journal.load()
            if current is None:
                raise DirectWardenError("restore journal disappeared")
            journal.rollback_restore(
                operation_id=restoring.operation_id,
                expected_revision=current.revision,
                candidate_reaped=True,
            )
        finally:
            if candidate is not None:
                candidate.close()

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
        )

    def _runtime_fingerprint(
        self,
        sandbox: DirectSandbox,
    ) -> HibernationRuntimeFingerprint:
        return replace(
            self.config.runtime_fingerprint,
            rootfs_sha256=sandbox.rootfs_sha256,
        )

    def _state_identity(self, sandbox: DirectSandbox) -> tuple[int, int]:
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
        return (
            str(self.config.runsc),
            f"--root={self.config.runtime_root}",
            f"--platform={self.config.platform}",
            f"--network={self.config.network}",
            f"--application-memory-file-dir={self.config.memory_root}",
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

    def _journal(self, sandbox: DirectSandbox) -> HibernationJournal:
        return self.journals.journal(
            sandbox_id=sandbox.sandbox_id,
            sandbox_generation=sandbox.sandbox_generation,
        )

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
