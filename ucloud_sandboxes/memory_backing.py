"""Quota-owned ordinary files for live memory and execution checkpoints.

One project-quota filesystem serves the node. This store owns allocation and
reclamation, not sandbox lifecycle, capture generations or execution authority.
"""

from __future__ import annotations

from contextlib import closing, contextmanager
from concurrent.futures import Future
from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import struct
import subprocess
from threading import Lock, RLock, local
import time

from opentelemetry.trace import get_current_span

from .checkpoint_components import MemoryBackingRef
from .durable_batch import DurableSqliteBatch


@dataclass(frozen=True)
class MemoryBackingLease:
    reference: MemoryBackingRef
    sandbox_id: str
    sandbox_generation: int
    project_id: int
    path: Path
    active_mode: str = "file"


class MemoryBackingError(RuntimeError):
    pass


class MemoryBackingBusyError(MemoryBackingError):
    pass


@dataclass(frozen=True)
class RetainedCheckpointRef:
    reference: MemoryBackingRef
    hibernation_generation: int
    manifest_sha256: str


class FilesystemTrim:
    """Coalesce already-unlinked allocations behind a shared physical barrier.

    Arrivals after a pass starts need the next pass: a running FITRIM may have
    already scanned their extents. Every caller retains its journal claim until
    its own pass succeeds. No background task or second recovery journal exists.
    """

    def __init__(self, trim, *, settle_seconds=0.01):
        self._trim = trim
        self._settle_seconds = settle_seconds
        self._guard = Lock()
        self._serial = Lock()
        self._batch = None

    def release(self):
        with self._guard:
            owner = self._batch is None
            if owner:
                self._batch = Future()
            batch = self._batch
        if not owner:
            return batch.result()
        try:
            time.sleep(self._settle_seconds)
            with self._serial:
                with self._guard:
                    self._batch = None
                self._trim()
        except BaseException as exc:
            with self._guard:
                if self._batch is batch:
                    self._batch = None
            batch.set_exception(exc)
            raise
        else:
            batch.set_result(None)


class XfsMemoryQuota:
    """Small Linux boundary; admission never falls back to an unbounded folder."""

    def __init__(self):
        self._trim = FilesystemTrim(self._trim_filesystem)

    def _trim_filesystem(self):
        subprocess.run(
            ["fstrim", str(self.filesystem_root)],
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )

    def validate_root(self, root: Path) -> None:
        result = subprocess.run(
            ["findmnt", "-n", "-o", "FSTYPE,OPTIONS", "--target", str(root)],
            text=True,
            capture_output=True,
            check=True,
        ).stdout.split()
        if (
            len(result) != 2
            or result[0] != "xfs"
            or not ({"prjquota", "pquota"} & set(result[1].split(",")))
        ):
            raise MemoryBackingError(
                "memory backing requires an XFS project-quota filesystem"
            )
        # xfs_quota accepts a mountpoint, not an arbitrary directory within it;
        # it can report an unrecognized path without a nonzero exit status.
        self.filesystem_root = Path(
            subprocess.run(
                ["findmnt", "-n", "-o", "TARGET", "--target", str(root)],
                text=True,
                capture_output=True,
                check=True,
            ).stdout.strip()
        )
        if not self.filesystem_root.is_absolute():
            raise MemoryBackingError("memory backing mountpoint is invalid")

    def provision(
        self, root: Path, path: Path, project_id: int, quota_bytes: int
    ) -> None:
        # Both paths and identity are store-controlled, never guest shell text.
        for command in (
            f"project -s -p {path} {project_id}",
            f"limit -p bsoft={quota_bytes} bhard={quota_bytes} {project_id}",
        ):
            subprocess.run(
                ["xfs_quota", "-x", "-c", command, str(self.filesystem_root)],
                check=True,
                capture_output=True,
                text=True,
            )
        self.validate_project(path, project_id)

    def validate_project(self, path: Path, project_id: int) -> None:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            # FS_IOC_FSGETXATTR, struct fsxattr. Read actual kernel ownership.
            payload = fcntl.ioctl(fd, 0x801C581F, bytes(28))
            flags, _, _, actual, _ = struct.unpack("=IIIII8x", payload)
            if actual != project_id or not flags & 0x200:  # FS_XFLAG_PROJINHERIT
                raise MemoryBackingError(
                    "memory directory lost project-quota ownership"
                )
        finally:
            os.close(fd)

    def retain_file(self, path: Path, project_id: int, quota_bytes: int) -> None:
        """Move an immutable source's charge off the live application's quota."""
        phase_started = time.monotonic()
        subprocess.run(
            ["xfs_quota", "-x", "-c",
             f"limit -p bsoft={quota_bytes} bhard={quota_bytes} {project_id}",
             str(self.filesystem_root)],
            check=True, capture_output=True, text=True,
        )
        get_current_span().add_event("memory.retain.quota_limit",
            {"duration_ms": (time.monotonic() - phase_started) * 1000})
        phase_started = time.monotonic()
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                raise MemoryBackingError("retained memory is not an owned regular file")
            attributes = bytearray(fcntl.ioctl(fd, 0x801C581F, bytes(28)))
            struct.pack_into("=I", attributes, 12, project_id)
            fcntl.ioctl(fd, 0x401C5820, attributes)  # FS_IOC_FSSETXATTR
            actual = struct.unpack("=IIIII8x", fcntl.ioctl(fd, 0x801C581F, bytes(28)))[3]
            if actual != project_id:
                raise MemoryBackingError("retained memory project assignment failed")
            get_current_span().add_event("memory.retain.assign_project",
                {"duration_ms": (time.monotonic() - phase_started) * 1000})
            phase_started = time.monotonic()
            os.fsync(fd)
            get_current_span().add_event("memory.retain.sync_inode",
                {"duration_ms": (time.monotonic() - phase_started) * 1000})
        finally:
            os.close(fd)

    def release(self, root: Path, project_id: int) -> None:
        self.release_many(root, (project_id,))

    def release_many(self, root: Path, project_ids) -> None:
        projects = tuple(dict.fromkeys(project_ids))
        if not projects:
            return
        source = subprocess.run(
            ["findmnt", "-n", "-o", "SOURCE", "--target", str(self.filesystem_root)],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if source.startswith("/dev/loop"):
            # XFS free space inside a sparse loop image is not yet free space
            # on the shared parent device. Keep the hard claim until FITRIM has
            # synchronously punched those image extents back to the parent.
            self._trim.release()
        for project_id in projects:
            subprocess.run(
                ["xfs_quota", "-x", "-c", f"limit -p bsoft=0 bhard=0 {project_id}",
                 str(self.filesystem_root)],
                check=True, capture_output=True, text=True,
            )


class MemoryBackingStore:
    MARKER = ".memory-owner.json"

    def __init__(
        self,
        root: Path,
        journal: Path,
        *,
        hard_capacity_bytes: int,
        quota: XfsMemoryQuota | None = None,
        active_root: Path | None = None,
    ) -> None:
        if (
            not root.is_absolute()
            or not journal.is_absolute()
            or hard_capacity_bytes <= 0
        ):
            raise ValueError("memory backing paths/capacity are invalid")
        # xfs_quota's command parser does not implement shell quoting.
        if any(char.isspace() for char in str(root)):
            raise ValueError("memory backing root cannot contain whitespace")
        self.active_root = active_root
        if active_root is not None:
            if not active_root.is_absolute() or active_root == root:
                raise ValueError("RAM memory root must be a distinct absolute path")
            self._private_directory(active_root)
            filesystem = subprocess.run(
                ["findmnt", "-n", "-o", "FSTYPE,OPTIONS", "--target", str(active_root)],
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()
            fields = filesystem.split()
            if (
                len(fields) != 2
                or fields[0] != "tmpfs"
                or "noswap" not in fields[1].split(",")
            ):
                raise MemoryBackingError(
                    "active RAM backing requires tmpfs with noswap"
                )
        self.root = root
        self.journal = journal
        self.hard_capacity_bytes = hard_capacity_bytes
        self.quota = quota or XfsMemoryQuota()
        # SQLite serializes journal writers and each owner's flock fences its
        # filesystem side effects. No node-wide lock spans subprocesses or
        # fsyncs, so one create cannot stall unrelated wakes and reclaims.
        self._readers = local()
        self._active_modes_lock = RLock()
        self._active_modes: dict[tuple[str, int], str] = {}
        self.lease_root = journal.parent / (journal.name + ".leases")
        self.lease_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._private_directory(self.lease_root)
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        journal.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._private_directory(root)
        self.quota.validate_root(root)
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute("PRAGMA user_version").fetchone()[0] not in {0, 1, 2}:
                raise MemoryBackingError("unsupported memory backing journal version")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS allocations ("
                "allocation_id TEXT PRIMARY KEY, sandbox_id TEXT NOT NULL, "
                "generation INTEGER NOT NULL, project_id INTEGER UNIQUE NOT NULL, "
                "quota_bytes INTEGER NOT NULL, state TEXT NOT NULL)"
            )
            conn.execute("CREATE TABLE IF NOT EXISTS counter (value INTEGER NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS features (name TEXT PRIMARY KEY)")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS retained_checkpoints ("
                "allocation_id TEXT NOT NULL, hibernation_generation INTEGER NOT NULL, "
                "manifest_sha256 TEXT NOT NULL, allocated_bytes INTEGER NOT NULL, "
                "device INTEGER NOT NULL, inode INTEGER NOT NULL, "
                "project_id INTEGER UNIQUE NOT NULL, state TEXT NOT NULL, "
                "PRIMARY KEY(allocation_id,hibernation_generation))"
            )
            columns = {row[1] for row in conn.execute("PRAGMA table_info(allocations)")}
            if "active_mode" not in columns:
                # This journal predates owner-local backing placement. Its old
                # layout was selected once for the entire worker at bootstrap.
                conn.execute("ALTER TABLE allocations ADD COLUMN active_mode TEXT NOT NULL DEFAULT 'file'")
                if active_root is not None:
                    conn.execute("UPDATE allocations SET active_mode='ram'")
            if conn.execute("SELECT COUNT(*) FROM counter").fetchone()[0] == 0:
                conn.execute("INSERT INTO counter VALUES (600000)")
            # Old readers cannot interpret per-owner placement. Fence them
            # before any allocation can transition away from their RAM root.
            conn.execute("PRAGMA user_version=2")
            conn.commit()
            for sandbox_id, generation, mode in conn.execute(
                "SELECT sandbox_id,generation,active_mode FROM allocations WHERE state!='deleted'"
            ):
                self._remember_mode(sandbox_id, generation, mode)

        self._journal_pid = os.getpid()
        info = self.journal.stat()
        self._journal_identity = (info.st_dev, info.st_ino)
        self._write_batches = DurableSqliteBatch(self._connect, self._check_journal_identity)

    def _check_journal_identity(self) -> None:
        if os.getpid() != self._journal_pid:
            raise MemoryBackingError("memory journal must be reopened after fork")
        info = self.journal.stat()
        if (info.st_dev, info.st_ino) != self._journal_identity:
            raise MemoryBackingError("memory journal file was replaced")

    def _remember_mode(self, sandbox_id: str, generation: int, mode: str) -> None:
        if mode not in {"ram", "file"} or (mode == "ram" and self.active_root is None):
            raise MemoryBackingError("memory allocation has an unsupported active backing mode")
        with self._active_modes_lock:
            self._active_modes[(sandbox_id, generation)] = mode

    def _forget_mode(self, sandbox_id: str, generation: int) -> None:
        with self._active_modes_lock:
            self._active_modes.pop((sandbox_id, generation), None)

    def active_mode(self, sandbox_id: str, generation: int) -> str | None:
        """Cached placement evidence, never permission to change a live runtime.

        Placement is monotonic from RAM to file within an incarnation. An older
        overlapping reader can only retain the more conservative RAM forecast;
        create/restore revalidate durable ownership before launching a runtime.
        """
        # Admission reads this while holding its node-wide capacity guard.
        # Never make that guard wait for allocator filesystem or journal I/O.
        with self._active_modes_lock:
            return self._active_modes.get((sandbox_id, generation))

    def configure_reflink_restore(self, enabled: bool) -> None:
        """Keep the required reader until all its allocations have drained.

        Placement and checkpoint ownership must never silently revert to the
        old destructive restore semantics when a deployment flag is removed.
        """
        with self._write_batches.transaction() as conn:
            conn.execute("BEGIN IMMEDIATE")
            previous = conn.execute(
                "SELECT 1 FROM features WHERE name='reflink-restore-v1'"
            ).fetchone()
            retained = conn.execute(
                "SELECT 1 FROM allocations WHERE state!='deleted' LIMIT 1"
            ).fetchone()
            retained = retained or conn.execute(
                "SELECT 1 FROM retained_checkpoints WHERE state!='deleted' LIMIT 1"
            ).fetchone()
            if previous and not enabled and retained:
                raise MemoryBackingError("reflink restore reader is required until retained allocations drain")
            if enabled:
                conn.execute("INSERT OR IGNORE INTO features VALUES ('reflink-restore-v1')")
            else:
                conn.execute("DELETE FROM features WHERE name='reflink-restore-v1'")
            conn.commit()

    def _connect(self):
        conn = sqlite3.connect(self.journal, timeout=30, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _reader(self) -> sqlite3.Connection:
        """This thread's long-lived read connection; statements autocommit.

        Reopening per call made the last close checkpoint and delete the WAL,
        and the next open recreate it, on every lifecycle read.
        """
        self._check_journal_identity()
        conn = getattr(self._readers, "connection", None)
        if conn is None:
            conn = sqlite3.connect(self.journal, timeout=30)
            self._readers.connection = conn
        return conn

    @staticmethod
    def _private_directory(path: Path) -> None:
        info = path.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o077
        ):
            raise MemoryBackingError("memory backing directory is not privately owned")

    def prepare(
        self, reference: MemoryBackingRef, *, sandbox_id: str, sandbox_generation: int
    ) -> MemoryBackingLease:
        expected_id = f"{sandbox_id}.sandbox-{sandbox_generation}"
        if reference.allocation_id != expected_id or sandbox_generation < 1:
            raise MemoryBackingError("memory allocation belongs to another incarnation")
        with self._mutation_lock(reference):
            row, created = self._claim_allocation(
                reference, sandbox_id=sandbox_id, sandbox_generation=sandbox_generation
            )
            if not created and (
                row[1:3] != (sandbox_id, sandbox_generation)
                or row[4] != reference.quota_bytes
                or row[5] == "deleted"
            ):
                raise MemoryBackingError(
                    "memory allocation identity/claim conflicts"
                )
            lease = MemoryBackingLease(
                reference,
                sandbox_id,
                sandbox_generation,
                row[3],
                self.root / reference.allocation_id,
                row[6],
            )
            if row[5] == "deleting":
                raise MemoryBackingError("memory allocation is being deleted")
            if row[5] == "ready":
                self._validate(lease)
                return lease
            lease.path.mkdir(mode=0o700, exist_ok=True)
            self._private_directory(lease.path)
            marker = lease.path / self.MARKER
            data = self._marker(lease)
            if marker.exists():
                if marker.is_symlink() or json.loads(marker.read_text()) != data:
                    raise MemoryBackingError("memory allocation marker conflicts")
            else:
                # A crash between mkdir and marker is recoverable only while empty.
                if any(lease.path.iterdir()):
                    raise MemoryBackingError("unmarked memory allocation is not empty")
                fd = os.open(
                    marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
                )
                with os.fdopen(fd, "w") as stream:
                    json.dump(data, stream, sort_keys=True)
                    stream.flush()
                    os.fsync(stream.fileno())
                self._sync(lease.path)
                self._sync(self.root)
            self.quota.provision(
                self.root, lease.path, lease.project_id, reference.quota_bytes
            )
            with self._write_batches.transaction() as conn:
                conn.execute(
                    "UPDATE allocations SET state='ready' WHERE allocation_id=? AND state='preparing'",
                    (reference.allocation_id,),
                )
                conn.commit()
            self._prepare_active(lease)
            self._remember_mode(sandbox_id, sandbox_generation, lease.active_mode)
            return lease

    def _claim_allocation(
        self, reference: MemoryBackingRef, *, sandbox_id: str, sandbox_generation: int
    ) -> tuple[tuple, bool]:
        """Reserve capacity and a project ID in one durable journal commit."""
        created = False
        with self._write_batches.transaction() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM allocations WHERE allocation_id=?",
                (reference.allocation_id,),
            ).fetchone()
            active_mode = "ram" if self.active_root is not None else "file"
            if row is not None and row[5] == "deleted":
                if (
                    row[1:3] != (sandbox_id, sandbox_generation)
                    or row[4] != reference.quota_bytes
                ):
                    raise MemoryBackingError(
                        "reimported memory identity/claim conflicts"
                    )
                if os.path.lexists(self.root / reference.allocation_id):
                    raise MemoryBackingError(
                        "deleted memory allocation path still exists"
                    )
                conn.execute(
                    "DELETE FROM allocations WHERE allocation_id=?",
                    (reference.allocation_id,),
                )
                # Reimporting the same incarnation must not reverse placement:
                # old readers may still hold its conservative cached evidence.
                active_mode = row[6]
                row = None
            if row is None:
                reserved = conn.execute(
                    "SELECT COALESCE(SUM(quota_bytes),0) FROM allocations WHERE state!='deleted'"
                ).fetchone()[0]
                if reserved + reference.quota_bytes > self.hard_capacity_bytes:
                    raise MemoryBackingError("memory backing hard capacity exhausted")
                project = conn.execute("SELECT value FROM counter").fetchone()[0]
                conn.execute("UPDATE counter SET value=value+1")
                row = (
                    reference.allocation_id,
                    sandbox_id,
                    sandbox_generation,
                    project,
                    reference.quota_bytes,
                    "preparing",
                    active_mode,
                )
                conn.execute("INSERT INTO allocations VALUES (?,?,?,?,?,?,?)", row)
                created = True
            conn.commit()
        return row, created


    def require(
        self, reference: MemoryBackingRef, *, sandbox_id: str, sandbox_generation: int
    ) -> MemoryBackingLease:
        with self._mutation_lock(reference):
            row = self._reader().execute(
                "SELECT * FROM allocations WHERE allocation_id=?",
                (reference.allocation_id,),
            ).fetchone()
            if (
                row is None
                or row[1:3] != (sandbox_id, sandbox_generation)
                or row[4] != reference.quota_bytes
                or row[5] != "ready"
            ):
                raise MemoryBackingError(
                    "memory allocation is not retained by this incarnation"
                )
            lease = MemoryBackingLease(
                reference,
                sandbox_id,
                sandbox_generation,
                row[3],
                self.root / reference.allocation_id,
                row[6],
            )
            self._validate(lease)
            return lease

    def _validate(self, lease: MemoryBackingLease) -> None:
        self._private_directory(lease.path)
        marker = lease.path / self.MARKER
        if marker.is_symlink() or json.loads(marker.read_text()) != self._marker(lease):
            raise MemoryBackingError("memory allocation marker conflicts")
        self.quota.validate_project(lease.path, lease.project_id)
        self._prepare_active(lease)
        self._remember_mode(lease.sandbox_id, lease.sandbox_generation, lease.active_mode)

    def _prepare_active(self, lease: MemoryBackingLease) -> None:
        if self.active_root is None or lease.active_mode != "ram":
            return
        path = self.active_root / lease.reference.allocation_id
        path.mkdir(mode=0o700, exist_ok=True)
        self._private_directory(path)

    def prepare_file_restore(
        self, reference: MemoryBackingRef, *, sandbox_id: str, sandbox_generation: int
    ) -> MemoryBackingLease:
        """Persist file placement before a fenced parked owner's next launch.

        The Warden must already own PARKED authority. This allocator validates
        the retained quota and refuses to abandon any still-present RAM files;
        it neither checkpoints nor moves a live mapping. The selection survives
        candidate failure and restart, so recovery chooses the same backing.
        """
        with self._mutation_lock(reference):
            # The owner's mutation lock keeps this row stable until the update.
            row = self._reader().execute(
                "SELECT * FROM allocations WHERE allocation_id=?", (reference.allocation_id,)
            ).fetchone()
            if (row is None or row[1:3] != (sandbox_id, sandbox_generation)
                    or row[4] != reference.quota_bytes or row[5] != "ready"):
                raise MemoryBackingError("file restore does not own a retained memory allocation")
            lease = MemoryBackingLease(reference, sandbox_id, sandbox_generation,
                                       row[3], self.root / reference.allocation_id, row[6])
            self._validate(lease)
            if lease.active_mode == "ram":
                active = self.active_root / reference.allocation_id
                if any(active.iterdir()):
                    raise MemoryBackingError("file restore cannot abandon live RAM backing")
                with self._write_batches.transaction() as conn:
                    conn.execute("UPDATE allocations SET active_mode='file' WHERE allocation_id=?",
                                 (reference.allocation_id,))
                    conn.commit()
            self._remember_mode(sandbox_id, sandbox_generation, "file")
            return MemoryBackingLease(reference, sandbox_id, sandbox_generation,
                                      row[3], lease.path, "file")

    def retain_checkpoint(
        self, reference: MemoryBackingRef, *, sandbox_id: str,
        sandbox_generation: int, hibernation_generation: int,
        manifest_sha256: str, allocated_bytes: int,
    ) -> None:
        """Assign immutable memory its separately admitted temporary quota.

        The caller reserves these bytes in the canonical registry BEFORE this
        operation and owns PARKED lifecycle authority. The live project never
        receives extra space that a guest could consume before cleanup.
        """
        if hibernation_generation < 1 or allocated_bytes < 1:
            raise MemoryBackingError("retained checkpoint capacity is invalid")
        phase_started = time.monotonic()
        lease = self.require(reference, sandbox_id=sandbox_id,
                             sandbox_generation=sandbox_generation)
        path = lease.path / f"hibernate-{hibernation_generation}" / "application_memory.img"
        with self._mutation_lock(reference):
            self._private_directory(path.parent)
            info = path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077 or info.st_blocks * 512 > allocated_bytes):
                raise MemoryBackingError("retained checkpoint identity or capacity differs")
            get_current_span().add_event("memory.retain.validate",
                {"duration_ms": (time.monotonic() - phase_started) * 1000})
            phase_started = time.monotonic()
            # The per-owner mutation lease fences filesystem side effects.
            # SQLite serializes counter/row updates; a shared FULL commit lets
            # independent owners retain concurrently without a node-wide lock.
            with self._write_batches.transaction() as conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT manifest_sha256,allocated_bytes,device,inode,project_id,state "
                    "FROM retained_checkpoints WHERE allocation_id=? AND hibernation_generation=?",
                    (reference.allocation_id, hibernation_generation),
                ).fetchone()
                expected = (manifest_sha256, allocated_bytes, info.st_dev, info.st_ino)
                if row is not None and row[5] == "retiring":
                    raise MemoryBackingBusyError("retained checkpoint physical release is pending")
                if row is not None and row[5] == "deleted":
                    # A parked incarnation can migrate away and return without
                    # executing or advancing its hibernation generation. The
                    # old source/project was fully retired; the caller has
                    # already admitted this new source in the global registry.
                    # Never recycle a project ID or reinterpret an active row.
                    conn.execute(
                        "DELETE FROM retained_checkpoints WHERE allocation_id=? AND hibernation_generation=?",
                        (reference.allocation_id, hibernation_generation),
                    )
                    row = None
                if row is None:
                    project = conn.execute("SELECT value FROM counter").fetchone()[0]
                    conn.execute("UPDATE counter SET value=value+1")
                    conn.execute("INSERT INTO retained_checkpoints VALUES (?,?,?,?,?,?,?,?)",
                                 (reference.allocation_id, hibernation_generation,
                                  *expected, project, "preparing"))
                elif row[:4] != expected:
                    raise MemoryBackingError("retained checkpoint quota identity conflicts")
                else:
                    project = row[4]
                conn.commit()
            get_current_span().add_event("memory.retain.prepare_journal",
                {"duration_ms": (time.monotonic() - phase_started) * 1000})
            # Idempotent through a crash before/after inode reassignment. Only
            # this owner's mutation lease is held over filesystem operations.
            self.quota.retain_file(path, project, allocated_bytes)
            phase_started = time.monotonic()
            with self._write_batches.transaction() as conn:
                conn.execute("UPDATE retained_checkpoints SET state='ready' "
                             "WHERE allocation_id=? AND hibernation_generation=?",
                             (reference.allocation_id, hibernation_generation))
                conn.commit()

            get_current_span().add_event("memory.retain.ready_journal",
                {"duration_ms": (time.monotonic() - phase_started) * 1000})

    def release_retained_checkpoint(
        self, reference: MemoryBackingRef, *, hibernation_generation: int,
        manifest_sha256: str,
    ) -> bool:
        """Release only after artifact deletion, closed readers and physical trim."""
        with self._mutation_lock(reference), self._allocation_lock(reference, exclusive=True):
            path = self.root / reference.allocation_id / f"hibernate-{hibernation_generation}"
            if os.path.lexists(path):
                return False
            row = self._reader().execute(
                "SELECT manifest_sha256,project_id,state FROM retained_checkpoints "
                "WHERE allocation_id=? AND hibernation_generation=?",
                (reference.allocation_id, hibernation_generation),
            ).fetchone()
            if row is None:
                # A crash may follow the global claim but precede project setup.
                return True
            if row[0] != manifest_sha256:
                raise MemoryBackingError("retained checkpoint release identity conflicts")
            if row[2] != "deleted":
                self.quota.release(self.root, row[1])
                with self._write_batches.transaction() as conn:
                    conn.execute("UPDATE retained_checkpoints SET state='deleted' "
                                 "WHERE allocation_id=? AND hibernation_generation=?",
                                 (reference.allocation_id, hibernation_generation))
                    conn.commit()
            return True

    def release_retained_checkpoints(self, checkpoints, *, release_claim) -> int:
        """Batch the physical barrier without holding any lifecycle/owner lock.

        Existing retention rows fence each exact project while trim is outside
        the locks. A concurrent deletion can finish that project; final identity
        checks prevent the old batch from touching a reimported replacement.
        The global claim is always last, after the local deleted commit.
        """
        pending = []
        for checkpoint in checkpoints:
            ref = checkpoint.reference
            identity = (ref.allocation_id, checkpoint.hibernation_generation)
            try:
                with self._mutation_lock(ref), self._allocation_lock(ref, exclusive=True):
                    path = self.root / ref.allocation_id / f"hibernate-{checkpoint.hibernation_generation}"
                    if os.path.lexists(path):
                        continue
                    with self._write_batches.transaction() as conn:
                        conn.execute("BEGIN IMMEDIATE")
                        row = conn.execute(
                            "SELECT manifest_sha256,allocated_bytes,device,inode,project_id,state "
                            "FROM retained_checkpoints WHERE allocation_id=? AND hibernation_generation=?",
                            identity,
                        ).fetchone()
                        if row is not None:
                            if row[0] != checkpoint.manifest_sha256:
                                raise MemoryBackingError("retained checkpoint release identity conflicts")
                            if row[5] not in {"preparing", "ready", "retiring", "deleted"}:
                                raise MemoryBackingError("retained checkpoint release state is invalid")
                            if row[5] != "deleted":
                                conn.execute("UPDATE retained_checkpoints SET state='retiring' "
                                             "WHERE allocation_id=? AND hibernation_generation=?", identity)
                        conn.commit()
                    pending.append((checkpoint, row))
            except MemoryBackingBusyError:
                continue
        projects = [row[4] for _, row in pending if row is not None and row[5] != "deleted"]
        self.quota.release_many(self.root, projects)
        released = 0
        for checkpoint, before in pending:
            ref = checkpoint.reference
            identity = (ref.allocation_id, checkpoint.hibernation_generation)
            try:
                with self._mutation_lock(ref), self._allocation_lock(ref, exclusive=True):
                    path = self.root / ref.allocation_id / f"hibernate-{checkpoint.hibernation_generation}"
                    if os.path.lexists(path):
                        continue
                    with self._write_batches.transaction() as conn:
                        conn.execute("BEGIN IMMEDIATE")
                        current = conn.execute(
                            "SELECT manifest_sha256,allocated_bytes,device,inode,project_id,state "
                            "FROM retained_checkpoints WHERE allocation_id=? AND hibernation_generation=?",
                            identity,
                        ).fetchone()
                        if ((before is None and current is not None)
                                or (before is not None and (current is None or current[:5] != before[:5]
                                    or current[5] not in {"retiring", "deleted"}))):
                            continue
                        if current is not None:
                            conn.execute("UPDATE retained_checkpoints SET state='deleted' "
                                         "WHERE allocation_id=? AND hibernation_generation=?", identity)
                        conn.commit()
                    release_claim(checkpoint)
                    released += 1
            except MemoryBackingBusyError:
                continue
        return released

    @staticmethod
    def _marker(lease: MemoryBackingLease):
        return {
            "allocation_id": lease.reference.allocation_id,
            "project_id": lease.project_id,
            "quota_bytes": lease.reference.quota_bytes,
            "sandbox_id": lease.sandbox_id,
            "sandbox_generation": lease.sandbox_generation,
            "version": 1,
        }

    @staticmethod
    def _sync(path: Path):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def delete(
        self, reference: MemoryBackingRef, *, sandbox_id: str, sandbox_generation: int
    ) -> None:
        """Called only after the lifecycle owner has fenced every runtime."""
        with self._mutation_lock(reference):
            # The owner's mutation lock keeps this row stable until the update.
            row = self._reader().execute(
                "SELECT * FROM allocations WHERE allocation_id=?",
                (reference.allocation_id,),
            ).fetchone()
            if row is None:
                # A planned registration may fail before allocation starts.
                if os.path.lexists(self.root / reference.allocation_id):
                    raise MemoryBackingError(
                        "unregistered memory allocation path exists"
                    )
                return
            if (
                row[1:3] != (sandbox_id, sandbox_generation)
                or row[4] != reference.quota_bytes
            ):
                raise MemoryBackingError("memory deletion identity conflicts")
            if row[5] == "deleted":
                self._forget_mode(sandbox_id, sandbox_generation)
                return
            lease = MemoryBackingLease(
                reference,
                sandbox_id,
                sandbox_generation,
                row[3],
                self.root / reference.allocation_id,
                row[6],
            )
            if row[5] != "deleting":
                if row[5] == "preparing":
                    if os.path.lexists(lease.path):
                        self._private_directory(lease.path)
                        marker = lease.path / self.MARKER
                        if os.path.lexists(marker):
                            if marker.is_symlink() or json.loads(
                                marker.read_text()
                            ) != self._marker(lease):
                                raise MemoryBackingError(
                                    "memory allocation marker conflicts"
                                )
                        elif any(lease.path.iterdir()):
                            raise MemoryBackingError(
                                "unmarked memory allocation is not empty"
                            )
                else:
                    self._validate(lease)
                with self._write_batches.transaction() as conn:
                    conn.execute(
                        "UPDATE allocations SET state='deleting' WHERE allocation_id=?",
                        (reference.allocation_id,),
                    )
                    conn.commit()
            with self._allocation_lock(reference, exclusive=True):
                if lease.path.exists():
                    self._private_directory(lease.path)
                    shutil.rmtree(lease.path)
                    self._sync(self.root)
                if self.active_root is not None:
                    active = self.active_root / reference.allocation_id
                    if active.exists():
                        self._private_directory(active)
                        shutil.rmtree(active)
                self.quota.release(self.root, lease.project_id)
                with self._write_batches.transaction() as conn:
                    conn.execute(
                        "UPDATE allocations SET state='deleted' WHERE allocation_id=? AND state='deleting'",
                        (reference.allocation_id,),
                    )
                    conn.commit()
                self._forget_mode(sandbox_id, sandbox_generation)

    @contextmanager
    def _mutation_lock(self, reference: MemoryBackingRef):
        # Serialize durable preparing/deleting state and filesystem side effects
        # across a service restart overlap, independently of publication readers.
        path = self.lease_root / (reference.allocation_id + ".mutation")
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @contextmanager
    def _allocation_lock(self, reference: MemoryBackingRef, *, exclusive: bool):
        # Outside the allocation so unlink/reimport cannot create another lock
        # inode while a publisher still holds a physical backing file open.
        path = self.lease_root / reference.allocation_id
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(
                    fd, (fcntl.LOCK_EX | fcntl.LOCK_NB) if exclusive else fcntl.LOCK_SH
                )
            except BlockingIOError as exc:
                raise MemoryBackingBusyError(
                    "memory allocation still has publication readers"
                ) from exc
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @contextmanager
    def read_lease(
        self, reference: MemoryBackingRef, *, sandbox_id: str, sandbox_generation: int
    ):
        """Retain the hard claim until the publisher closes every source FD."""
        with self._allocation_lock(reference, exclusive=False):
            lease = self.require(
                reference, sandbox_id=sandbox_id, sandbox_generation=sandbox_generation
            )
            yield lease

    def metrics(self) -> dict[str, int]:
        count, reserved = self._reader().execute(
            "SELECT COUNT(*),COALESCE(SUM(quota_bytes),0) FROM allocations WHERE state!='deleted'"
        ).fetchone()
        return {
            "memory_backing_allocations": count,
            "memory_backing_hard_reserved_bytes": reserved,
        }
