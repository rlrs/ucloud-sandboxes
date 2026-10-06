"""The exec fence the agent shares with runtime/noded through the kernel (phase 2a).

docs/rust-node-daemon-plan.md, phase 2. With ``--rust-execs`` noded runs execs
on running, unpaused sandboxes without asking the agent, so the agent's
in-process lifecycle coordinator cannot see them. Two flock files per sandbox
id (the coordinator is keyed by id, not generation), in the Warden's lock
directory, carry the fence across processes:

- ``.<id>.transition`` (T): a Python lifecycle transition holds it exclusively.
  noded takes it shared and non-blocking only while it takes A; a busy T sends
  the exec to the agent, which joins the transition as today.
- ``.<id>.activity`` (A): noded holds it shared for a whole exec session, from
  before its running check until the session is reaped. Park and pause need it
  exclusively and fail fast; delete and wake never take it, so they tolerate
  running execs exactly as the in-process ``allow_shared`` does.

Lock order is T, then A, everywhere. A's mtime is the activity clock: noded
touches it when an exec starts and completes, and the agent's own activity
marks touch it too.

Delete unlinks A, then T, while it holds T exclusively. A process that opened
a file before the unlink would lock an orphan inode, so every locker checks,
after it has the lock, that the path still names its descriptor's inode, and
opens again otherwise.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import logging
import os
from pathlib import Path
from threading import Lock
from typing import Iterator

from .sandbox import SANDBOX_ID_RE, SandboxBusyError

_LOG = logging.getLogger(__name__)
_FLAGS = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)


class ExecFence:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        # Ids this process holds T for; only a holder may discard the files.
        self._held: set[str] = set()
        self._held_guard = Lock()

    def transition_path(self, sandbox_id: str) -> Path:
        return self._path(sandbox_id, "transition")

    def activity_path(self, sandbox_id: str) -> Path:
        return self._path(sandbox_id, "activity")

    def _path(self, sandbox_id: str, kind: str) -> Path:
        if not SANDBOX_ID_RE.fullmatch(sandbox_id):
            raise ValueError("sandbox id is invalid")
        return self.directory / f".{sandbox_id}.{kind}"

    @contextmanager
    def transition(self, sandbox_id: str, *, allow_shared: bool) -> Iterator[None]:
        """Hold T (and A unless ``allow_shared``) for one lifecycle transition.

        Call it after the in-process check, which already excludes this
        process's other transitions for the id. noded holds T for microseconds
        and never blocks while holding it, so waiting for T is bounded.
        """
        transition = _locked(self.transition_path(sandbox_id), fcntl.LOCK_EX)
        assert transition is not None
        activity = None
        try:
            if not allow_shared:
                activity = _locked(self.activity_path(sandbox_id), fcntl.LOCK_EX | fcntl.LOCK_NB)
                if activity is None:
                    raise SandboxBusyError(f"sandbox has active exec/file activity: {sandbox_id}")
            with self._held_guard:
                self._held.add(sandbox_id)
            try:
                yield
            finally:
                with self._held_guard:
                    self._held.discard(sandbox_id)
        finally:
            if activity is not None:
                os.close(activity)
            os.close(transition)

    def discard(self, sandbox_id: str) -> None:
        """Unlink a deleted sandbox's fence files; the caller holds its T.

        A goes first: while the linked T is held, nobody can take A through
        the protocol, so no exec of a later incarnation can hold the old A.
        An exec of the deleted one may still hold it; delete severs those.
        """
        with self._held_guard:
            if sandbox_id not in self._held:
                raise RuntimeError("discarding exec fence files needs the transition lock")
        for path in (self.activity_path(sandbox_id), self.transition_path(sandbox_id)):
            path.unlink(missing_ok=True)

    def activity_idle(self, sandbox_id: str) -> bool:
        """Probe A without keeping it: an observation, never authority."""
        try:
            descriptor = os.open(self.activity_path(sandbox_id), _FLAGS)
        except FileNotFoundError:
            return True  # No exec has made it, and an exec holds it from before its start.
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            return True
        finally:
            os.close(descriptor)

    def touch(self, sandbox_id: str) -> None:
        """Advance the activity clock (futimens: mtime := now). Never fails."""
        try:
            descriptor = os.open(self.activity_path(sandbox_id), _FLAGS | os.O_CREAT, 0o600)
            try:
                os.utime(descriptor)
            finally:
                os.close(descriptor)
        except OSError as exc:
            # The agent's monotonic mark still holds its own activity.
            _LOG.warning("activity clock of %s not touched: %s", sandbox_id, exc)

    def activity_mtime_ns(self, sandbox_id: str) -> int | None:
        """A's last touch, in wall-clock nanoseconds; None without a file."""
        try:
            return os.stat(self.activity_path(sandbox_id), follow_symlinks=False).st_mtime_ns
        except FileNotFoundError:
            return None


def _locked(path: Path, operation: int) -> int | None:
    """Open ``path`` and flock it; None if non-blocking and busy.

    Retries while the lock landed on an inode the path no longer names: a
    delete unlinked it (and maybe a later opener made a new one) meanwhile.
    """
    while True:
        descriptor = os.open(path, _FLAGS | os.O_CREAT, 0o600)
        try:
            try:
                fcntl.flock(descriptor, operation)
            except BlockingIOError:
                os.close(descriptor)
                return None
            held = os.fstat(descriptor)
            try:
                current = os.stat(path, follow_symlinks=False)
            except FileNotFoundError:
                current = None
            if current is not None and (current.st_dev, current.st_ino) == (held.st_dev, held.st_ino):
                return descriptor
        except BaseException:
            os.close(descriptor)
            raise
        os.close(descriptor)
