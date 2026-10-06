"""The owner's in-memory registry index against the SQLite journal.

The node agent's registry instance owns the file system-wide and serves reads
from memory. The index must equal the committed state: never ahead of a
COMMIT, never missing another connection's commit at the owner's next write,
rebuilt after ownership moves to a new instance or process.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread
import time
from unittest.mock import patch
import os
import signal
import sqlite3
import subprocess
import sys
import unittest

from ucloud_sandboxes import direct_registry
from ucloud_sandboxes.direct_registry import (
    DirectRegistryConflictError,
    DirectRegistryError,
    DirectSandboxRegistry,
    _RegistryConnection,
)
# Import the module, not the TestCase: discovery would rerun it here.
from tests import test_direct_registry as registry_fixtures

TEST_TIER = "contract"
REPO = Path(__file__).resolve().parents[1]


class _FailsOnce:
    """A connection whose next ``method`` call reports an I/O error.

    A failing COMMIT takes effect first; a failing ROLLBACK does nothing, so
    the write transaction stays open.
    """

    def __init__(self, connection: sqlite3.Connection, method: str) -> None:
        self._connection, self._method = connection, method

    def __getattr__(self, name):
        attribute = getattr(self._connection, name)
        if name != self._method:
            return attribute

        def fail():
            self._method = None
            if name == "commit":
                attribute()
            raise sqlite3.OperationalError("disk I/O error")

        return fail

    @staticmethod
    def install(owner: DirectSandboxRegistry, method: str) -> None:
        entry = owner._owner_entry
        owner._owner_entry = _RegistryConnection(
            _FailsOnce(entry.connection, method), entry.schema_stamp)


class _FailsStatement:
    """A connection whose next statement starting with ``prefix`` reports an I/O error."""

    def __init__(self, connection: sqlite3.Connection, prefix: str) -> None:
        self._connection, self._prefix = connection, prefix

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def execute(self, sql, *args):
        if self._prefix and sql.startswith(self._prefix):
            self._prefix = ""
            raise sqlite3.OperationalError("disk I/O error")
        return self._connection.execute(sql, *args)


class RegistryIndexTests(unittest.TestCase):
    fixtures = registry_fixtures.DirectRegistryTests()

    def setUp(self) -> None:
        self._directory = TemporaryDirectory()
        self.addCleanup(self._directory.cleanup)
        self.root = Path(self._directory.name).resolve()
        self.path = self.root / "registry.sqlite"

    def owner(self) -> DirectSandboxRegistry:
        registry = DirectSandboxRegistry(self.path, owner=True)
        self.addCleanup(registry.close)
        return registry

    def plan(self, registry, name: str, generation: int = 1):
        return registry.plan(
            spec=self.fixtures.spec(name), sandbox_generation=generation,
            operation_id=f"create:{generation}", runtime_compatibility_sha256="b" * 64,
        )

    def assert_matches_journal(self, owner: DirectSandboxRegistry) -> None:
        journal = DirectSandboxRegistry(self.path)
        self.assertEqual(owner.snapshot().records, journal.snapshot().records)
        self.assertEqual(owner.activity_revision(), journal.activity_revision())
        self.assertEqual(owner.disk_claims_mb(), journal.disk_claims_mb())

    def test_reads_never_run_ahead_of_commit(self) -> None:
        owner = self.owner()
        planned = self.plan(owner, "a")
        journal = DirectSandboxRegistry(self.path)
        written, release = Event(), Event()
        original = owner._write

        def write_then_wait(connection, record, **kwargs):
            original(connection, record, **kwargs)
            written.set()
            release.wait(10)

        owner._write = write_then_wait
        writer = Thread(target=owner.commit_quota, args=("a",), kwargs=dict(
            expected_revision=planned.revision, project_id=200_001, total_mb=4096,
            quota_path=self.root / "a"))
        writer.start()
        self.assertTrue(written.wait(10))
        # Written but not committed: no reader may see it, the owner included.
        owner._index_checked_at = float("-inf")
        self.assertEqual(owner.get("a"), planned)
        self.assertEqual(owner.activity_revision(), planned.revision)
        self.assertEqual(journal.get("a"), planned)
        release.set()
        writer.join(10)
        # The write returned, so its own thread and later readers see it.
        self.assertEqual(owner.get("a").phase, "quota_ready")
        self.assert_matches_journal(owner)

    def test_commit_reaches_the_index_only_after_it_returns(self) -> None:
        owner = self.owner()
        planned = self.plan(owner, "a")
        applying, release = Event(), Event()
        original = direct_registry._RegistryIndex.applied

        def wait_then_apply(index, staged, revision):
            applying.set()
            release.wait(10)
            return original(index, staged, revision)

        with patch.object(direct_registry._RegistryIndex, "applied", wait_then_apply):
            writer = Thread(target=owner.begin_delete, args=("a",),
                            kwargs=dict(expected_revision=planned.revision))
            writer.start()
            self.assertTrue(applying.wait(10))
            # Committed, not yet applied: the owner is briefly behind, never ahead.
            self.assertEqual(DirectSandboxRegistry(self.path).get("a").phase, "deleting")
            self.assertEqual(owner.get("a"), planned)
            release.set()
            writer.join(10)
        self.assertEqual(owner.get("a").phase, "deleting")

    def test_queued_writers_share_one_commit_and_fail_alone(self) -> None:
        owner = self.owner()
        planned = self.plan(owner, "a")
        commits: list[str] = []
        owner._owner_entry.connection.set_trace_callback(
            lambda sql: commits.append(sql) if sql.upper().startswith("COMMIT") else None)
        inside, release, results = Event(), Event(), {}
        original = owner._write

        def write_then_wait(connection, record, **kwargs):
            original(connection, record, **kwargs)
            if record.sandbox_id == "a":
                inside.set()
                release.wait(10)

        def run(name, call):
            try:
                results[name] = call()
            except Exception as exc:  # noqa: BLE001 - the failure is the result
                results[name] = exc

        owner._write = write_then_wait
        threads = [Thread(target=run, args=("a", lambda: owner.commit_quota(
            "a", expected_revision=planned.revision, project_id=200_001, total_mb=4096,
            quota_path=self.root / "a")))]
        threads[0].start()
        self.assertTrue(inside.wait(10))
        threads += [Thread(target=run, args=("b", lambda: self.plan(owner, "b"))),
                    Thread(target=run, args=("stale", lambda: owner.commit_owned("a", expected_revision=planned.revision)))]
        for thread in threads[1:]:
            thread.start()
        deadline = time.monotonic() + 10
        while owner._turn_waiters < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        release.set()
        for thread in threads:
            thread.join(10)
        self.assertEqual((results["a"].phase, results["b"].phase), ("quota_ready", "planned"))
        self.assertIsInstance(results["stale"], DirectRegistryConflictError)
        self.assertEqual(len(commits), 1)  # One fsync for the three writers.
        self.assert_matches_journal(owner)

    def test_failed_body_or_commit_leaves_the_index_equal_to_the_journal(self) -> None:
        owner = self.owner()
        planned = self.plan(owner, "a")
        with self.assertRaises(DirectRegistryConflictError):
            owner.commit_owned("a", expected_revision=planned.revision)  # wrong phase
        self.assertEqual(owner.get("a"), planned)
        # A COMMIT whose outcome is reported as failed drops the index.
        _FailsOnce.install(owner, "commit")
        with self.assertRaisesRegex(DirectRegistryError, "unreadable"):
            owner.begin_delete("a", expected_revision=planned.revision)
        self.assertIsNone(owner._index)
        deleting = owner.get("a")
        self.assertEqual(deleting.phase, "deleting")  # Reread, as committed.
        self.assert_matches_journal(owner)
        # A writer whose savepoint cannot be rolled back leaves the shared
        # transaction untrustworthy: it is abandoned and the connection dropped.
        entry = owner._owner_entry
        owner._owner_entry = _RegistryConnection(_FailsStatement(entry.connection, "ROLLBACK TO"), entry.schema_stamp)
        with self.assertRaises(DirectRegistryConflictError):
            owner.commit_owned("a", expected_revision=deleting.revision)  # wrong phase
        self.assertIsNone(owner._owner_entry)
        self.plan(owner, "b")
        self.assertEqual(owner._owner_entry.connection.execute("PRAGMA synchronous").fetchone()[0], 2)
        self.assert_matches_journal(owner)

    def test_concurrent_owner_and_foreign_writers_stay_coherent(self) -> None:
        owner = self.owner()
        foreign = DirectSandboxRegistry(self.path)
        stop = Event()
        failures: list[str] = []

        def lifecycle(registry, prefix: str) -> None:
            for index in range(8):
                name = f"{prefix}{index}"
                if index % 2:
                    self.fixtures.delete_registration(registry, self.root, name, 1)
                else:
                    self.fixtures.owned_registration(registry, self.root, name, 1)

        def read() -> None:
            last = 0
            while not stop.is_set():
                snapshot = owner.snapshot()
                revision = snapshot.activity_revision
                if revision < last:
                    failures.append(f"revision went back from {last} to {revision}")
                last = revision
                if owner.activity_revision() < revision:
                    failures.append("clock behind its own snapshot")
                for record in snapshot.records:
                    current = owner.get(record.sandbox_id)
                    if current is not None and current.revision < record.revision:
                        failures.append(f"{record.sandbox_id} went back")
                time.sleep(0.001)  # Spinning readers would starve writers of the GIL.

        readers = [Thread(target=read) for _ in range(3)]
        for reader in readers:
            reader.start()
        try:
            with ThreadPoolExecutor(max_workers=5) as pool:
                jobs = [pool.submit(lifecycle, owner, f"o{index}-") for index in range(4)]
                jobs.append(pool.submit(lifecycle, foreign, "f-"))
                for job in jobs:
                    job.result()
        finally:
            stop.set()
            for reader in readers:
                reader.join(10)
        self.assertEqual(failures, [])
        owner._index_checked_at = float("-inf")  # The foreign writer's last commit.
        self.assert_matches_journal(owner)
        self.assertEqual(len(owner.snapshot().records), 20)

    def test_owner_write_sees_raw_sql_and_reads_recheck_it(self) -> None:
        owner = self.owner()
        planned = self.plan(owner, "a")
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("DELETE FROM registration_disk")
            connection.execute("DELETE FROM registrations")
            connection.commit()
        with self.assertRaisesRegex(DirectRegistryConflictError, "absent"):
            owner.begin_delete("a", expected_revision=planned.revision)
        self.assertIsNone(owner.get("a"))
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("PRAGMA ignore_check_constraints = ON")
            connection.execute("UPDATE registry_metadata SET activity_revision = -1")
            connection.commit()
        owner._index_checked_at = float("-inf")
        with self.assertRaisesRegex(DirectRegistryError, "metadata"):
            owner.get("a")

    def test_owner_refuses_a_replaced_file_and_a_shared_lock(self) -> None:
        owner = self.owner()
        self.plan(owner, "a")
        # The owner connection still has the old inode open: writing through
        # it would lose every later commit, and reads would serve its state.
        replacement = self.root / "replacement.sqlite"
        replacement.touch(mode=0o600)
        os.replace(replacement, self.path)
        with self.assertRaisesRegex(DirectRegistryError, "replaced"):
            self.plan(owner, "b")
        owner._index_checked_at = float("-inf")
        with self.assertRaisesRegex(DirectRegistryError, "replaced"):
            owner.get("a")
        owner.close()
        lock = self.path.with_name(self.path.name + ".owner")
        os.chmod(lock, 0o644)
        with self.assertRaisesRegex(DirectRegistryError, "owner lock must be private"):
            DirectSandboxRegistry(self.path, owner=True)

    def test_ownership_is_exclusive_and_moves_with_reopen(self) -> None:
        first = self.owner()
        self.plan(first, "a")
        with self.assertRaisesRegex(DirectRegistryError, "another live owner"):
            DirectSandboxRegistry(self.path, owner=True)
        # Non-owners still read and write through SQLite.
        self.plan(DirectSandboxRegistry(self.path), "b")
        before = DirectSandboxRegistry(self.path).snapshot()
        first.close()
        self.assertEqual(first.get("a"), before.get("a"))  # Straggler: plain SQLite.
        second = self.owner()
        self.assertEqual(second.snapshot(), before)  # Rebuilt from the journal.

    def test_ownership_dies_with_its_process(self) -> None:
        child = subprocess.Popen(
            [sys.executable, "-c", (
                "import sys, time\n"
                "from pathlib import Path\n"
                "from ucloud_sandboxes.direct_registry import DirectSandboxRegistry\n"
                "from tests.test_direct_registry import DirectRegistryTests\n"
                "owner = DirectSandboxRegistry(Path(sys.argv[1]), owner=True)\n"
                "owner.plan(spec=DirectRegistryTests().spec('child'), sandbox_generation=1,\n"
                "           operation_id='create:1', runtime_compatibility_sha256='b' * 64)\n"
                "print('ready', flush=True)\n"
                "time.sleep(60)\n"
            ), str(self.path)],
            cwd=REPO, stdout=subprocess.PIPE, text=True,
        )
        self.addCleanup(child.wait)
        self.addCleanup(child.kill)
        self.assertEqual(child.stdout.readline().strip(), "ready")
        with self.assertRaisesRegex(DirectRegistryError, "another live owner"):
            DirectSandboxRegistry(self.path, owner=True)
        self.assertEqual(DirectSandboxRegistry(self.path).get("child").phase, "planned")
        os.kill(child.pid, signal.SIGKILL)
        child.wait(10)
        owner = self.owner()
        self.assertEqual(owner.get("child").phase, "planned")

    def test_memoized_derivations_stay_out_of_the_record(self) -> None:
        owner = self.owner()
        record = self.fixtures.owned_registration(owner, self.root, "a", 1)
        cached = owner.get("a")
        self.assertIs(cached.to_direct_sandbox(), cached.to_direct_sandbox())
        self.assertEqual(cached.spec_sha256, record.spec_sha256)
        self.assertEqual(cached.to_dict(), record.to_dict())
        self.assertEqual(DirectSandboxRegistry._encode(cached), DirectSandboxRegistry._encode(record))
        self.assertEqual(type(cached).from_dict(cached.to_dict()), cached)

    def test_fork_child_must_reopen(self) -> None:
        owner = self.owner()
        self.plan(owner, "a")
        pid = os.fork()
        if pid == 0:
            try:
                owner.get("a")
            except DirectRegistryError:
                os._exit(0)
            os._exit(1)
        _, status = os.waitpid(pid, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        self.assertEqual(owner.get("a").phase, "planned")


if __name__ == "__main__":
    unittest.main()
