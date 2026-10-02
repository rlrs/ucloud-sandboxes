"""Bounded pipeline admission, including durable ownership during cleanup."""
from dataclasses import replace
import multiprocessing
import os
import json
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests.test_images import _uploaded_context
from ucloud_sandboxes.images import (
    DockerImageRuntime, ImageBuildCapacityError, ImageBuildConflictError,
    ImageBuildRecord, ImageBuildSpec, ImageBuildStore, ImageManager, ImageStore,
)
from ucloud_sandboxes.sandbox import CommandResult
from ucloud_sandboxes.build_deadline import build_execution_deadline

TEST_TIER = "contract"


def record(name, *, phase="preparing_solving", status="running", owner=None):
    return ImageBuildRecord(build_id=name, image_id=name, tag="local/" + name,
                            status=status, created_at="2026-09-29T00:00:00+00:00",
                            updated_at="2026-09-29T00:00:00+00:00",
                            request_fingerprint=name, admission_phase=phase,
                            owner_pid=os.getpid() if owner is None else owner)


def reserve_process(path, index, start, results):
    store = ImageBuildStore(Path(path))
    start.wait(5)
    try:
        store.reserve_build(record(str(index)), max_active_builds=6, max_preparing_builds=4)
    except ImageBuildCapacityError:
        results.put("full")
    else:
        results.put("accepted")


class ControlledRuntime(DockerImageRuntime):
    def __init__(self):
        super().__init__(buildx_direct_push=True)
        self.entered = {}
        self.release = {}
        self.lock = Lock()
        self.active = self.peak = 0

    def arm(self, name, *, release=False):
        self.entered[name], self.release[name] = Event(), Event()
        if release:
            self.release[name].set()

    def build(self, spec, *, push=False, on_output=None):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        self.entered[spec.id].set()
        try:
            if not self.release[spec.id].wait(5):
                raise TimeoutError("test did not release build")
            return CommandResult(argv=("fixture", spec.id), exit_code=0)
        finally:
            with self.lock:
                self.active -= 1


class PhaseAdmissionTests(unittest.TestCase):
    def setUp(self):
        # Replay the exact uploaded archive, including its gzip header bytes.
        # Repacking identical files on another clock second is a new identity.
        self.context_fixture = _uploaded_context(("Dockerfile", b"FROM scratch\n"))

    def manager(self, root, runtime, *, publisher=None, builds=4, finishing=2, **options):
        options.setdefault("max_queued_builds", 0)
        return ImageManager(ImageStore(root / "images.sqlite"), runtime,
                            max_active_builds=builds, max_finishing_builds=finishing,
                            environment_publisher=publisher, **options)

    def submit(self, manager, name, *, cleanup=None, materialize=None):
        identity, default_materialize = self.context_fixture
        return manager.start_build(ImageBuildSpec(id=name, tag="local/" + name, context_path="."),
                                   context_identity=identity,
                                   materialize_context=materialize or default_materialize,
                                   push=True, cleanup=cleanup)

    def wait(self, manager, row):
        done = manager.wait_for_build(row.build_id, timeout_seconds=5)
        self.assertTrue(done.terminal)
        return done

    def test_four_solves_overlap_two_finishers_without_accepting_a_seventh(self):
        with TemporaryDirectory() as temporary:
            runtime = ControlledRuntime()
            publish_entered = {name: Event() for name in ("f0", "f1")}
            publish_release = Event()
            def publish(spec):
                if spec.id in publish_entered:
                    publish_entered[spec.id].set()
                    if not publish_release.wait(5):
                        raise TimeoutError("publisher not released")
                return "sha256:" + "a" * 64
            manager = self.manager(Path(temporary), runtime, publisher=publish)
            rows = []
            try:
                for name in ("f0", "f1"):
                    runtime.arm(name, release=True)
                    rows.append(self.submit(manager, name)[0])
                    self.assertTrue(publish_entered[name].wait(2))
                self.assertEqual(manager.build_admission_snapshot(), {
                    "active_builds": 2, "preparing_solving_builds": 0,
                    "finishing_builds": 2, "available_build_slots": 4, "admission_capacity": 6})
                for name in ("s0", "s1", "s2", "s3"):
                    runtime.arm(name)
                    rows.append(self.submit(manager, name)[0])
                    self.assertTrue(runtime.entered[name].wait(2))
                snapshot = manager.build_admission_snapshot()
                self.assertEqual((snapshot["active_builds"], snapshot["preparing_solving_builds"],
                                  snapshot["finishing_builds"], snapshot["available_build_slots"]), (6, 4, 2, 0))
                with self.assertRaises(ImageBuildCapacityError):
                    self.submit(manager, "overflow")
                joined, accepted = self.submit(manager, "s0")
                self.assertFalse(accepted)
                self.assertEqual(joined.build_id, rows[2].build_id)
                identity, materialize = _uploaded_context(("Dockerfile", b"FROM different\n"))
                with self.assertRaises(ImageBuildConflictError):
                    manager.start_build(ImageBuildSpec(id="s0", tag="local/s0", context_path="."),
                                        context_identity=identity, materialize_context=materialize, push=True)
                self.assertLessEqual(runtime.peak, 4)
            finally:
                publish_release.set()
                for gate in runtime.release.values():
                    gate.set()
                for row in rows:
                    self.assertEqual(self.wait(manager, row).status, "succeeded")
            self.assertEqual(manager.active_build_count(), 0)

    def test_preparing_is_owned_before_worker_start_and_failed_preparation_releases(self):
        with TemporaryDirectory() as temporary:
            manager = self.manager(Path(temporary), ControlledRuntime(), builds=1, finishing=1)
            observed = []
            def fail():
                observed.append(manager.build_admission_snapshot())
                with self.assertRaises(ImageBuildCapacityError):
                    self.submit(manager, "second")
                raise ValueError("invalid context")
            with self.assertRaisesRegex(ValueError, "invalid context"):
                self.submit(manager, "first", materialize=fail)
            self.assertEqual(observed[0]["active_builds"], 1)
            self.assertEqual(observed[0]["preparing_solving_builds"], 1)
            self.assertEqual(observed[0]["available_build_slots"], 0)
            self.assertEqual(manager.active_build_count(), 0)
            self.assertEqual(manager.list_builds()[0].admission_phase, "released")

    def test_terminal_cleanup_still_owns_finish_slot_and_replay_joins(self):
        with TemporaryDirectory() as temporary:
            runtime = ControlledRuntime()
            runtime.arm("first", release=True)
            runtime.arm("second", release=True)
            cleanup_entered, cleanup_release = Event(), Event()
            def cleanup():
                cleanup_entered.set()
                cleanup_release.wait(5)
            manager = self.manager(Path(temporary), runtime, builds=1, finishing=1)
            rows = []
            try:
                rows.append(self.submit(manager, "first", cleanup=cleanup)[0])
                self.assertTrue(cleanup_entered.wait(2))
                self.assertTrue(manager.get_build(rows[0].build_id).terminal)
                self.assertEqual(manager.active_build_count(), 1)
                rows.append(self.submit(manager, "second")[0])
                self.assertTrue(runtime.entered["second"].wait(2))
                self.assertEqual(manager.build_admission_snapshot()["available_build_slots"], 0)
                with patch("gzip.time.time", return_value=1):
                    joined, accepted = self.submit(manager, "first")
                self.assertFalse(accepted)
                self.assertEqual(joined.build_id, rows[0].build_id)
                with self.assertRaises(ImageBuildCapacityError):
                    self.submit(manager, "third")
                self.assertEqual(manager.active_build_count(), 2)
            finally:
                cleanup_release.set()
                for row in rows:
                    self.wait(manager, row)
            self.assertEqual(manager.active_build_count(), 0)
            self.assertEqual(manager.get_build(rows[0].build_id).admission_phase, "released")

    def test_finish_slot_wait_consumes_execution_deadline_without_releasing_early(self):
        # The budget clock moves only when the test moves it, so host load
        # cannot spend the budget before the build reaches the finishing slot.
        now = [1000.0]
        clock = SimpleNamespace(monotonic=lambda: now[0])
        with TemporaryDirectory() as temporary, patch(
            "ucloud_sandboxes.build_deadline.time", clock
        ):
            runtime = ControlledRuntime()
            runtime.arm("first", release=True)
            runtime.arm("second", release=True)
            publisher_entered, publisher_release = Event(), Event()
            def publish(spec):
                if spec.id == "first":
                    publisher_entered.set()
                    publisher_release.wait(5)
                return "sha256:" + "a" * 64
            manager = self.manager(Path(temporary), runtime, builds=1, finishing=1,
                                   publisher=publish, build_execution_timeout_seconds=3)
            enter_finishing = manager.build_store.try_enter_finishing
            attempts, polled = [], Event()
            def observed(build_id, **kwargs):
                entered = enter_finishing(build_id, **kwargs)
                if build_id != first.build_id:
                    attempts.append(entered)
                    if len(attempts) >= 3:
                        polled.set()
                return entered
            first = self.submit(manager, "first")[0]
            try:
                self.assertTrue(publisher_entered.wait(2))
                manager.build_execution_timeout_seconds = 0.1
                with patch.object(manager.build_store, "try_enter_finishing", observed):
                    second = self.submit(manager, "second")[0]
                    self.assertTrue(polled.wait(5))
                    # Inside its budget the wait keeps polling the held slot.
                    self.assertFalse(manager.get_build(second.build_id).terminal)
                    now[0] += 0.1
                    done = self.wait(manager, second)
                self.assertEqual(done.status, "failed")
                self.assertIn("server execution deadline", done.error)
                self.assertIn("finishing_wait_ms", done.timings["phases"])
                self.assertNotIn(True, attempts)
                self.assertEqual(manager.active_build_count(), 1)
                self.assertEqual(manager.build_admission_snapshot()["available_build_slots"], 1)
            finally:
                publisher_release.set()
                self.wait(manager, first)

    def test_two_managers_share_phase_accounting_and_do_not_reset_live_ownership(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = ControlledRuntime()
            runtime.arm("first", release=True)
            runtime.arm("second")
            entered, release = Event(), Event()
            def publish(spec):
                entered.set()
                release.wait(5)
                return "sha256:" + "a" * 64
            first_manager = self.manager(root, runtime, builds=1, finishing=1, publisher=publish)
            first = self.submit(first_manager, "first")[0]
            try:
                self.assertTrue(entered.wait(2))
                second_manager = self.manager(root, runtime, builds=1, finishing=1)
                self.assertEqual(second_manager.build_admission_snapshot()["finishing_builds"], 1)
                second = self.submit(second_manager, "second")[0]
                with self.assertRaises(ImageBuildCapacityError):
                    self.submit(first_manager, "third")
                self.assertEqual(first_manager.active_build_count(), 2)
            finally:
                release.set()
                runtime.release["second"].set()
                self.wait(first_manager, first)
                if "second" in locals():
                    self.wait(second_manager, second)

    def test_thread_start_failure_releases_only_after_cleanup(self):
        with TemporaryDirectory() as temporary:
            manager = self.manager(Path(temporary), ControlledRuntime(), builds=1, finishing=1)
            observed = []
            with patch("ucloud_sandboxes.images.Thread.start", side_effect=RuntimeError("start failed")):
                with self.assertRaisesRegex(RuntimeError, "start failed"):
                    self.submit(manager, "first", cleanup=lambda: observed.append(manager.active_build_count()))
            self.assertEqual(observed, [1])
            self.assertEqual(manager.active_build_count(), 0)

    def test_store_atomic_admission_limits_preparing_across_processes(self):
        with TemporaryDirectory() as temporary:
            path = str(Path(temporary) / "images.sqlite")
            store = ImageBuildStore(Path(path))
            store.upsert(record("finisher", phase="finishing"))
            ctx = multiprocessing.get_context("fork")
            start, results = ctx.Event(), ctx.Queue()
            processes = [ctx.Process(target=reserve_process, args=(path, i, start, results)) for i in range(8)]
            try:
                for process in processes:
                    process.start()
                start.set()
                outcomes = [results.get(timeout=5) for _ in processes]
                self.assertEqual(outcomes.count("accepted"), 4)
                self.assertEqual(outcomes.count("full"), 4)
                self.assertEqual(store.admission_snapshot(), {
                    "active_builds": 5, "finishing_builds": 1, "preparing_solving_builds": 4})
            finally:
                for process in processes:
                    process.join(5)
                    if process.is_alive():
                        process.terminate()
                        process.join()
                results.close()

    def test_finishing_transfer_is_atomic_and_compaction_preserves_cleanup(self):
        with TemporaryDirectory() as temporary:
            path = Path(temporary) / "images.sqlite"
            first, second = ImageBuildStore(path, max_terminal_builds=0), ImageBuildStore(path, max_terminal_builds=0)
            first.upsert(record("first"))
            second.upsert(record("second"))
            self.assertTrue(first.try_enter_finishing("first", max_finishing_builds=1))
            self.assertFalse(second.try_enter_finishing("second", max_finishing_builds=1))
            first.upsert(replace(first.get("first"), status="succeeded"))
            self.assertIsNotNone(second.get("first"))
            self.assertFalse(second.try_enter_finishing("second", max_finishing_builds=1))
            first.upsert(replace(first.get("first"), admission_phase="released"))
            self.assertIsNone(second.get("first"))
            self.assertTrue(second.try_enter_finishing("second", max_finishing_builds=1))

    def test_dead_owner_cleanup_and_execution_ownership_are_reconciled(self):
        with TemporaryDirectory() as temporary:
            store = ImageBuildStore(Path(temporary) / "images.sqlite")
            store.upsert(record("terminal", phase="finishing", status="succeeded", owner=999999))
            store.upsert(record("running", owner=999999))
            store.upsert(record("live", phase="finishing", status="succeeded"))
            with patch("ucloud_sandboxes.images._pid_is_running", side_effect=lambda pid: pid == os.getpid()):
                interrupted = store.reconcile_interrupted()
            self.assertEqual([item.build_id for item in interrupted], ["running"])
            self.assertEqual(store.get("terminal").admission_phase, "released")
            self.assertEqual(store.get("terminal").status, "succeeded")
            self.assertEqual(store.admission_snapshot()["active_builds"], 1)

    def test_failed_release_write_is_retried_by_next_snapshot(self):
        with TemporaryDirectory() as temporary:
            manager = self.manager(Path(temporary), ControlledRuntime())
            manager.build_store.upsert(record("finished", phase="finishing", status="succeeded"))
            original = manager.build_store.upsert
            with patch.object(manager.build_store, "upsert", side_effect=OSError("disk unavailable")):
                with self.assertRaises(OSError):
                    with manager._build_lock:
                        manager._release_build_admission_locked("finished")
            self.assertEqual(manager.build_store.admission_snapshot()["active_builds"], 1)
            self.assertEqual(manager._pending_terminal_builds["finished"].admission_phase, "released")
            with patch.object(manager.build_store, "upsert", wraps=original):
                self.assertEqual(manager.build_admission_snapshot()["active_builds"], 0)
            self.assertEqual(manager._pending_terminal_builds, {})

    def test_failed_release_read_is_retried_by_next_snapshot(self):
        with TemporaryDirectory() as temporary:
            manager = self.manager(Path(temporary), ControlledRuntime())
            manager.build_store.upsert(record("finished", phase="finishing", status="succeeded"))
            with patch.object(manager.build_store, "get_exact", side_effect=OSError("read unavailable")):
                with self.assertRaises(OSError):
                    with manager._build_lock:
                        manager._release_build_admission_locked("finished")
            self.assertEqual(manager._pending_terminal_builds, {})
            self.assertEqual(manager._pending_admission_releases, {"finished"})
            self.assertEqual(manager.build_admission_snapshot()["active_builds"], 0)
            self.assertEqual(manager._pending_admission_releases, set())

    def test_snapshot_and_transfer_use_only_owned_index_and_exact_target(self):
        with TemporaryDirectory() as temporary:
            store = ImageBuildStore(Path(temporary) / "images.sqlite")
            store.upsert(record("legacy", phase=""))
            store.upsert(record("waiting"))
            with store._transaction(write=True) as conn:
                for i in range(256):
                    store._put(conn, replace(record(f"old-{i}", phase="released", status="succeeded"),
                                             log_tail="x" * (64 * 1024)))
                query = (f"SELECT {store._STATUS_SQL}, {store._PHASE_SQL}, {store._PHASE_TYPE_SQL}, COUNT(*) "
                         f"FROM {store._table} WHERE {store._OWNED_SQL} "
                         f"GROUP BY {store._STATUS_SQL}, {store._PHASE_SQL}, {store._PHASE_TYPE_SQL}")
                plan = list(conn.execute("EXPLAIN QUERY PLAN " + query))
                self.assertTrue(any("image_build_owned_phases" in row[3] for row in plan), plan)
            with patch.object(store, "_load", side_effect=AssertionError("decoded retained history")):
                self.assertEqual(store.admission_snapshot()["preparing_solving_builds"], 2)
                self.assertTrue(store.try_enter_finishing("waiting", max_finishing_builds=1))
                self.assertEqual(store.admission_snapshot()["finishing_builds"], 1)
            for bad_status, bad_phase in ((None, "released"), ("succeeded", None), ("running", "unknown")):
                bad = record("bad").to_dict() | {"status": bad_status, "admission_phase": bad_phase}
                with store._transaction(write=True) as conn:
                    conn.execute(f"INSERT OR REPLACE INTO {store._table} VALUES (?, ?)", ("bad", json.dumps(bad)))
                with self.assertRaises(ValueError):
                    store.admission_snapshot()

    def test_finishing_transfer_database_contention_is_bounded_by_execution_budget(self):
        with TemporaryDirectory() as temporary:
            store = ImageBuildStore(Path(temporary) / "images.sqlite")
            store.upsert(record("waiting"))
            blocker = sqlite3.connect(store.path, isolation_level=None)
            blocker.execute("BEGIN IMMEDIATE")
            try:
                started = time.monotonic()
                with build_execution_deadline(0.05), self.assertRaises(ValueError):
                    store.try_enter_finishing("waiting", max_finishing_builds=1)
                self.assertLess(time.monotonic() - started, 0.5)
            finally:
                blocker.rollback()
                blocker.close()
            self.assertEqual(store.get_exact("waiting").admission_phase, "preparing_solving")

    def test_legacy_record_and_default_mode_remain_compatible(self):
        payload = record("old", phase="").to_dict()
        self.assertNotIn("admission_phase", payload)
        self.assertEqual(ImageBuildRecord.from_dict(payload).admission_phase, "")
        payload["admission_phase"] = "not-a-stage"
        self.assertIsNone(ImageBuildRecord.from_dict(payload))
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            manager = ImageManager(ImageStore(root / "images.sqlite"), ControlledRuntime())
            self.assertEqual(manager.max_finishing_builds, 0)
            self.assertEqual(manager.build_admission_snapshot()["admission_capacity"], 4)
            with self.assertRaises(ValueError):
                self.manager(root, ControlledRuntime(), max_queued_builds=1)


if __name__ == "__main__":
    unittest.main()
