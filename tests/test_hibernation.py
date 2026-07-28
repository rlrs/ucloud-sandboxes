from dataclasses import replace
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.hibernation import (
    HibernationArtifactFile,
    HibernationArtifactStore,
    HibernationAuthority,
    HibernationCapacityError,
    HibernationCompatibilityError,
    HibernationConflictError,
    HibernationDiskLedger,
    HibernationError,
    HibernationFileRole,
    HibernationJournal,
    HibernationJournalStore,
    HibernationManifest,
    HibernationReconciler,
    HibernationRecoveryAction,
    HibernationRuntimeFingerprint,
    HibernationState,
    HibernationQuotaError,
    classify_hibernation_recovery,
    hibernation_disk_reservation_mb,
    hibernation_memory_backing_reservation_mb,
    hibernation_process_identity_matches,
    linux_process_start_time_ticks,
)


DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
CONTAINER_ID = "d" * 64
RUNSC_COMMIT = "e" * 40


class HibernationTests(unittest.TestCase):
    @staticmethod
    def _write_proc_identity(
        proc_root: Path,
        pid: int,
        start_time_ticks: int,
    ) -> None:
        process = proc_root / str(pid)
        process.mkdir(parents=True, exist_ok=True)
        suffix = ["S", *(["0"] * 18), str(start_time_ticks), "0"]
        (process / "stat").write_text(
            f"{pid} (runsc worker) " + " ".join(suffix) + "\n",
            encoding="ascii",
        )

    def _runtime(self) -> HibernationRuntimeFingerprint:
        return HibernationRuntimeFingerprint(
            runsc_sha256=DIGEST_A,
            runsc_commit=RUNSC_COMMIT,
            platform="systrap",
            architecture="x86_64",
            page_size=4096,
            cpu_features_sha256=DIGEST_B,
            boot_config_sha256=DIGEST_C,
            rootfs_sha256=DIGEST_A,
        )

    def _manifest(
        self, root: Path, *, operation_id: str = "park:1"
    ) -> HibernationManifest:
        paths = {
            HibernationFileRole.MAIN_MEMORY: root / "application_memory.img",
            HibernationFileRole.KERNEL_STATE: root / "checkpoint.img",
            HibernationFileRole.ALLOCATOR_METADATA: root / "pages_meta.img",
        }
        paths[HibernationFileRole.MAIN_MEMORY].write_bytes(b"memory")
        paths[HibernationFileRole.KERNEL_STATE].write_bytes(b"kernel")
        paths[HibernationFileRole.ALLOCATOR_METADATA].write_bytes(b"metadata")
        return HibernationManifest(
            sandbox_id="sandbox-1",
            sandbox_generation=7,
            hibernation_generation=1,
            operation_id=operation_id,
            spec_sha256=DIGEST_B,
            container_id=CONTAINER_ID,
            created_ns=1,
            runtime=self._runtime(),
            files=tuple(
                HibernationArtifactFile.from_path(path, role=role)
                for role, path in paths.items()
            ),
        )

    def test_manifest_round_trip_and_metadata_tampering(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            manifest = self._manifest(root)
            restored = HibernationManifest.from_dict(manifest.to_dict())
            self.assertEqual(restored, manifest)

            tampered = manifest.to_dict()
            tampered["sandbox_generation"] = 8
            with self.assertRaisesRegex(ValueError, "digest does not match"):
                HibernationManifest.from_dict(tampered)

    def test_manifest_requires_runtime_and_sandbox_compatibility(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manifest = self._manifest(Path(raw_dir))
            manifest.require_compatible(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                runtime_sha256=manifest.runtime.digest,
            )
            with self.assertRaises(HibernationCompatibilityError):
                manifest.require_compatible(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                    spec_sha256=DIGEST_B,
                    runtime_sha256=DIGEST_C,
                )

    def test_manifest_validates_exact_file_identity_without_hashing_contents(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            manifest = self._manifest(root)
            manifest.validate_files(root)

            memory = root / "application_memory.img"
            replacement = root / "replacement"
            replacement.write_bytes(memory.read_bytes())
            os.replace(replacement, memory)
            with self.assertRaisesRegex(
                HibernationCompatibilityError, "identity changed"
            ):
                manifest.validate_files(root)

    def test_manifest_rejects_symlinked_artifact(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            manifest = self._manifest(root)
            memory = root / "application_memory.img"
            target = root / "target"
            os.replace(memory, target)
            memory.symlink_to(target.name)
            with self.assertRaisesRegex(HibernationCompatibilityError, "safely open"):
                manifest.validate_files(root)

    def test_manifest_schema_is_strict(self) -> None:
        with TemporaryDirectory() as raw_dir:
            manifest = self._manifest(Path(raw_dir))
            raw = manifest.to_dict()
            raw["unexpected"] = True
            with self.assertRaisesRegex(ValueError, "invalid schema"):
                HibernationManifest.from_dict(raw)

    def test_artifact_store_adopts_and_durably_publishes_generation(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            store_root = root / "artifacts"
            active_root = root / "active"
            active_root.mkdir(mode=0o700)
            active_memory = active_root / "application_memory.img"
            active_memory.write_bytes(b"memory")
            store = HibernationArtifactStore(store_root.resolve())
            generation = store.prepare_generation(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                hibernation_generation=1,
            )
            adopted = store.adopt_file(
                active_root=active_root,
                active_name=active_memory.name,
                generation=generation,
                artifact_name=active_memory.name,
            )
            self.assertFalse(active_memory.exists())
            self.assertEqual(adopted.read_bytes(), b"memory")
            (generation / "checkpoint.img").write_bytes(b"kernel")
            (generation / "pages_meta.img").write_bytes(b"metadata")
            manifest = HibernationManifest(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                hibernation_generation=1,
                operation_id="park:1",
                spec_sha256=DIGEST_B,
                container_id=CONTAINER_ID,
                created_ns=1,
                runtime=self._runtime(),
                files=(
                    HibernationArtifactFile.from_path(
                        generation / "application_memory.img",
                        role=HibernationFileRole.MAIN_MEMORY,
                    ),
                    HibernationArtifactFile.from_path(
                        generation / "checkpoint.img",
                        role=HibernationFileRole.KERNEL_STATE,
                    ),
                    HibernationArtifactFile.from_path(
                        generation / "pages_meta.img",
                        role=HibernationFileRole.ALLOCATOR_METADATA,
                    ),
                ),
            )

            store.publish_complete(manifest)
            self.assertEqual(
                store.load_complete(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                    hibernation_generation=1,
                ),
                manifest,
            )
            self.assertEqual(
                store.inventory()[0].metadata_sha256,
                manifest.metadata_sha256,
            )

    def test_artifact_store_never_treats_pending_as_complete(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = HibernationArtifactStore((Path(raw_dir) / "artifacts").resolve())
            store.prepare_generation(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                hibernation_generation=1,
            )
            self.assertEqual(store.inventory()[0].state, "pending")
            with self.assertRaisesRegex(
                HibernationCompatibilityError, "COMPLETE is absent"
            ):
                store.load_complete(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                    hibernation_generation=1,
                )

    def test_ignored_overlay_root_can_be_traversable_but_not_writable(self) -> None:
        with TemporaryDirectory() as raw_dir:
            store = HibernationArtifactStore(
                (Path(raw_dir) / "artifacts").resolve()
            )
            store.root.mkdir(mode=0o700)
            incarnation = store.root / "sandbox-1.sandbox-7"
            incarnation.mkdir(mode=0o700)
            upper = incarnation / "upper"
            upper.mkdir(mode=0o755)

            self.assertEqual(
                store.inventory_incarnation(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                    ignored_entries=("upper",),
                ),
                (),
            )
            upper.chmod(0o777)
            with self.assertRaisesRegex(HibernationError, "private and owned"):
                store.inventory_incarnation(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                    ignored_entries=("upper",),
                )

    def test_artifact_store_discards_only_pending_generation(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            store = HibernationArtifactStore((root / "artifacts").resolve())
            generation = store.prepare_generation(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                hibernation_generation=1,
            )
            (generation / "checkpoint.img").write_bytes(b"pending")
            store.discard_pending(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                hibernation_generation=1,
            )
            self.assertFalse(generation.exists())

            generation = store.prepare_generation(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                hibernation_generation=2,
            )
            manifest = self._manifest(generation)
            manifest = replace(manifest, hibernation_generation=2)
            store.publish_complete(manifest)
            with self.assertRaisesRegex(
                HibernationConflictError,
                "cannot discard a complete",
            ):
                store.discard_pending(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                    hibernation_generation=2,
                )

    def test_artifact_store_complete_marker_fences_manifest_replacement(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            store = HibernationArtifactStore((root / "artifacts").resolve())
            generation = store.prepare_generation(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                hibernation_generation=1,
            )
            manifest = self._manifest(generation)
            store.publish_complete(manifest)
            marker = generation / "COMPLETE"
            raw_marker = json.loads(marker.read_text(encoding="utf-8"))
            raw_marker["metadata_sha256"] = DIGEST_C
            marker.write_text(json.dumps(raw_marker), encoding="utf-8")
            with self.assertRaises(HibernationCompatibilityError):
                store.load_complete(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                    hibernation_generation=1,
                )

    def test_artifact_store_rejects_symlinked_generation(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            store_root = root / "artifacts"
            store = HibernationArtifactStore(store_root.resolve())
            generation = store.prepare_generation(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                hibernation_generation=1,
            )
            generation.rmdir()
            generation.symlink_to(root)
            with self.assertRaisesRegex(Exception, "real directory"):
                store.prepare_generation(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                    hibernation_generation=1,
                )

    def test_journal_hibernate_restore_lifecycle(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            journal = HibernationJournal((root / "journal.json").resolve())
            running = journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            self.assertEqual(running.state, HibernationState.RUNNING)

            hibernating = journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            self.assertEqual(hibernating.authority, HibernationAuthority.LIVE)
            pending = journal.mark_sentry_reaped(
                operation_id="park:1",
                expected_revision=hibernating.revision,
            )
            self.assertEqual(pending.authority, HibernationAuthority.PENDING)

            manifest = self._manifest(root)
            parked = journal.commit_parked(
                manifest,
                operation_id="park:1",
                expected_revision=pending.revision,
            )
            self.assertEqual(parked.state, HibernationState.PARKED)

            restoring = journal.begin_restore(
                operation_id="wake:1",
                expected_revision=parked.revision,
            )
            candidate = journal.mark_candidate_started(
                operation_id="wake:1",
                expected_revision=restoring.revision,
                candidate_pid=202,
                candidate_start_time_ticks=2002,
            )
            resumed = journal.commit_running(
                operation_id="wake:1",
                expected_revision=candidate.revision,
                sentry_pid=202,
                sentry_start_time_ticks=2002,
            )
            self.assertEqual(resumed.state, HibernationState.RUNNING)
            self.assertEqual(resumed.authority, HibernationAuthority.LIVE)
            self.assertEqual(resumed.hibernation_generation, 1)
            self.assertEqual(resumed.manifest_sha256, "")

    def test_journal_never_aborts_after_sentry_reap(self) -> None:
        with TemporaryDirectory() as raw_dir:
            journal = HibernationJournal((Path(raw_dir) / "journal.json").resolve())
            running = journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            hibernating = journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            pending = journal.mark_sentry_reaped(
                operation_id="park:1",
                expected_revision=hibernating.revision,
            )
            with self.assertRaisesRegex(HibernationConflictError, "cannot be aborted"):
                journal.abort_hibernate(
                    operation_id="park:1",
                    expected_revision=pending.revision,
                    sentry_pid=101,
                    sentry_start_time_ticks=1001,
                )

    def test_journal_requires_candidate_reap_before_rollback(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            journal = HibernationJournal((root / "journal.json").resolve())
            running = journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            hibernating = journal.begin_hibernate(
                operation_id="park:1", expected_revision=running.revision
            )
            pending = journal.mark_sentry_reaped(
                operation_id="park:1", expected_revision=hibernating.revision
            )
            parked = journal.commit_parked(
                self._manifest(root),
                operation_id="park:1",
                expected_revision=pending.revision,
            )
            restoring = journal.begin_restore(
                operation_id="wake:1", expected_revision=parked.revision
            )
            candidate = journal.mark_candidate_started(
                operation_id="wake:1",
                expected_revision=restoring.revision,
                candidate_pid=202,
                candidate_start_time_ticks=2002,
            )
            with self.assertRaisesRegex(
                HibernationConflictError, "candidate may be alive"
            ):
                journal.rollback_restore(
                    operation_id="wake:1",
                    expected_revision=candidate.revision,
                )
            rolled_back = journal.rollback_restore(
                operation_id="wake:1",
                expected_revision=candidate.revision,
                candidate_reaped=True,
            )
            self.assertEqual(rolled_back.state, HibernationState.PARKED)
            self.assertTrue(rolled_back.manifest_sha256)

    def test_journal_compare_and_swap_and_operation_replay(self) -> None:
        with TemporaryDirectory() as raw_dir:
            journal = HibernationJournal((Path(raw_dir) / "journal.json").resolve())
            running = journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            hibernating = journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            replay = journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            self.assertEqual(replay, hibernating)
            with self.assertRaisesRegex(
                HibernationConflictError, "stale hibernation revision"
            ):
                journal.mark_sentry_reaped(
                    operation_id="park:1",
                    expected_revision=running.revision,
                )

    def test_journal_rejects_another_incarnation(self) -> None:
        with TemporaryDirectory() as raw_dir:
            journal = HibernationJournal((Path(raw_dir) / "journal.json").resolve())
            journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            with self.assertRaises(HibernationConflictError):
                journal.initialize_running(
                    sandbox_id="sandbox-1",
                    sandbox_generation=8,
                    spec_sha256=DIGEST_B,
                    operation_id="create:8",
                    sentry_pid=102,
                    sentry_start_time_ticks=1002,
                )

    def test_journal_rejects_corruption_and_unsafe_parent(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            path = (root / "journal.json").resolve()
            path.write_text("{not-json", encoding="utf-8")
            with self.assertRaisesRegex(Exception, "journal is invalid"):
                HibernationJournal(path).load()

            path.unlink()
            root.chmod(0o777)
            try:
                with self.assertRaisesRegex(Exception, "group/world writable"):
                    HibernationJournal(path).load()
            finally:
                root.chmod(0o700)

    def test_recovery_classifier_covers_irreversible_boundaries(self) -> None:
        with TemporaryDirectory() as raw_dir:
            journal = HibernationJournal((Path(raw_dir) / "journal.json").resolve())
            running = journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            self.assertEqual(
                classify_hibernation_recovery(
                    running,
                    sentry_alive=True,
                    candidate_alive=False,
                    complete_manifest=False,
                ),
                HibernationRecoveryAction.ADOPT_RUNNING,
            )
            hibernating = journal.begin_hibernate(
                operation_id="park:1", expected_revision=running.revision
            )
            self.assertEqual(
                classify_hibernation_recovery(
                    hibernating,
                    sentry_alive=False,
                    candidate_alive=False,
                    complete_manifest=False,
                ),
                HibernationRecoveryAction.FINISH_PENDING_GENERATION,
            )

    def test_complete_capture_with_live_sentry_must_finish_stopping(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            journal = HibernationJournal((root / "journal.json").resolve())
            running = journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            hibernating = journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            self.assertEqual(
                classify_hibernation_recovery(
                    hibernating,
                    sentry_alive=True,
                    candidate_alive=False,
                    complete_manifest=True,
                ),
                HibernationRecoveryAction.FINISH_PUBLISHED_GENERATION,
            )

    def test_recovery_finishes_complete_artifact_after_journal_commit_crash(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            journal = HibernationJournal((root / "journal.json").resolve())
            artifact_store = HibernationArtifactStore((root / "artifacts").resolve())
            running = journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            hibernating = journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            pending = journal.mark_sentry_reaped(
                operation_id="park:1",
                expected_revision=hibernating.revision,
            )
            generation = artifact_store.prepare_generation(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                hibernation_generation=1,
            )
            manifest = self._manifest(generation)
            artifact_store.publish_complete(manifest)

            # Simulate process death after COMPLETE became durable but before
            # the journal moved from hibernating/pending to parked.
            restarted_journal = HibernationJournal((root / "journal.json").resolve())
            result = HibernationReconciler(
                restarted_journal,
                artifact_store,
                runtime_sha256=self._runtime().digest,
                proc_root=root / "empty-proc",
            ).reconcile()
            self.assertEqual(result.action, HibernationRecoveryAction.KEEP_PARKED)
            self.assertTrue(result.changed)
            self.assertEqual(result.record.state, HibernationState.PARKED)
            self.assertGreater(result.record.revision, pending.revision)

    def test_reconciler_quarantines_reused_or_missing_live_process(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            journal = HibernationJournal((root / "journal.json").resolve())
            artifacts = HibernationArtifactStore((root / "artifacts").resolve())
            journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            result = HibernationReconciler(
                journal,
                artifacts,
                runtime_sha256=self._runtime().digest,
                proc_root=root / "empty-proc",
            ).reconcile()
            self.assertEqual(result.action, HibernationRecoveryAction.QUARANTINE)
            self.assertEqual(
                result.record.state,
                HibernationState.RECOVERY_REQUIRED,
            )
            self.assertEqual(result.record.authority, HibernationAuthority.NONE)
            self.assertIsNone(result.record.sentry_pid)

    def test_reconciler_rolls_dead_candidate_back_to_complete_artifact(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            journal = HibernationJournal((root / "journal.json").resolve())
            artifacts = HibernationArtifactStore((root / "artifacts").resolve())
            running = journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            hibernating = journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            pending = journal.mark_sentry_reaped(
                operation_id="park:1",
                expected_revision=hibernating.revision,
            )
            generation = artifacts.prepare_generation(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                hibernation_generation=1,
            )
            manifest = self._manifest(generation)
            artifacts.publish_complete(manifest)
            parked = journal.commit_parked(
                manifest,
                operation_id="park:1",
                expected_revision=pending.revision,
            )
            restoring = journal.begin_restore(
                operation_id="wake:1",
                expected_revision=parked.revision,
            )
            journal.mark_candidate_started(
                operation_id="wake:1",
                expected_revision=restoring.revision,
                candidate_pid=202,
                candidate_start_time_ticks=2002,
            )

            result = HibernationReconciler(
                journal,
                artifacts,
                runtime_sha256=self._runtime().digest,
                proc_root=root / "empty-proc",
            ).reconcile()
            self.assertEqual(result.action, HibernationRecoveryAction.KEEP_PARKED)
            self.assertEqual(result.record.state, HibernationState.PARKED)
            self.assertEqual(result.record.authority, HibernationAuthority.PARKED)

    def test_reconciler_adopts_candidate_started_before_identity_commit(
        self,
    ) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            proc_root = root / "proc"
            proc_root.mkdir()
            journal = HibernationJournal((root / "journal.json").resolve())
            artifacts = HibernationArtifactStore((root / "artifacts").resolve())
            running = journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            hibernating = journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            pending = journal.mark_sentry_reaped(
                operation_id="park:1",
                expected_revision=hibernating.revision,
            )
            generation = artifacts.prepare_generation(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                hibernation_generation=1,
            )
            manifest = self._manifest(generation)
            artifacts.publish_complete(manifest)
            parked = journal.commit_parked(
                manifest,
                operation_id="park:1",
                expected_revision=pending.revision,
            )
            restoring = journal.begin_restore(
                operation_id="wake:1",
                expected_revision=parked.revision,
            )
            self._write_proc_identity(proc_root, 202, 2002)
            resolver_calls = 0

            def resolve_candidate(_record):
                nonlocal resolver_calls
                resolver_calls += 1
                return (202, 2002)

            reconciler = HibernationReconciler(
                journal,
                artifacts,
                runtime_sha256=self._runtime().digest,
                proc_root=proc_root,
                candidate_identity_resolver=resolve_candidate,
            )
            result = reconciler.reconcile()

            self.assertEqual(
                result.action,
                HibernationRecoveryAction.VERIFY_CANDIDATE,
            )
            self.assertTrue(result.changed)
            self.assertEqual(result.record.authority, HibernationAuthority.CANDIDATE)
            self.assertEqual(result.record.candidate_pid, 202)
            self.assertGreater(result.record.revision, restoring.revision)
            self.assertEqual(resolver_calls, 1)

            replay = reconciler.reconcile()
            self.assertEqual(
                replay.action,
                HibernationRecoveryAction.VERIFY_CANDIDATE,
            )
            self.assertFalse(replay.changed)
            self.assertEqual(resolver_calls, 1)

    def test_journal_store_reconciles_complete_startup_inventory(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            proc_root = root / "proc"
            proc_root.mkdir()
            store = HibernationJournalStore((root / "journals").resolve())
            artifacts = HibernationArtifactStore((root / "artifacts").resolve())
            for index in range(2):
                pid = 101 + index
                ticks = 1001 + index
                store.journal(
                    sandbox_id=f"sandbox-{index}",
                    sandbox_generation=7,
                ).initialize_running(
                    sandbox_id=f"sandbox-{index}",
                    sandbox_generation=7,
                    spec_sha256=DIGEST_B,
                    operation_id=f"create:{index}",
                    sentry_pid=pid,
                    sentry_start_time_ticks=ticks,
                )
                self._write_proc_identity(proc_root, pid, ticks)

            results = store.reconcile_all(
                artifacts,
                runtime_sha256=self._runtime().digest,
                proc_root=proc_root,
            )

            self.assertEqual(len(results), 2)
            self.assertTrue(
                all(
                    item.action == HibernationRecoveryAction.ADOPT_RUNNING
                    and not item.changed
                    for item in results
                )
            )

    def test_journal_store_refuses_orphan_artifact_generation(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            store = HibernationJournalStore((root / "journals").resolve())
            artifacts = HibernationArtifactStore((root / "artifacts").resolve())
            artifacts.prepare_generation(
                sandbox_id="orphan",
                sandbox_generation=7,
                hibernation_generation=1,
            )

            with self.assertRaisesRegex(
                HibernationConflictError,
                "without an owning journal",
            ):
                store.reconcile_all(
                    artifacts,
                    runtime_sha256=self._runtime().digest,
                    proc_root=root / "proc",
                )

    def test_reconciler_rejects_complete_artifact_from_another_runtime(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            journal = HibernationJournal((root / "journal.json").resolve())
            artifacts = HibernationArtifactStore((root / "artifacts").resolve())
            running = journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            hibernating = journal.begin_hibernate(
                operation_id="park:1",
                expected_revision=running.revision,
            )
            journal.mark_sentry_reaped(
                operation_id="park:1",
                expected_revision=hibernating.revision,
            )
            generation = artifacts.prepare_generation(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                hibernation_generation=1,
            )
            artifacts.publish_complete(self._manifest(generation))

            result = HibernationReconciler(
                journal,
                artifacts,
                runtime_sha256=DIGEST_C,
                proc_root=root / "empty-proc",
            ).reconcile()
            self.assertEqual(result.action, HibernationRecoveryAction.QUARANTINE)
            self.assertEqual(
                result.record.state,
                HibernationState.RECOVERY_REQUIRED,
            )
            self.assertIn("incompatible", result.detail)

    def test_disk_reservation_matches_allocator_growth_and_never_uses_holes(
        self,
    ) -> None:
        self.assertEqual(hibernation_memory_backing_reservation_mb(256), 1024)
        self.assertEqual(hibernation_memory_backing_reservation_mb(1024), 2048)
        self.assertEqual(hibernation_memory_backing_reservation_mb(4096), 5120)
        self.assertEqual(
            hibernation_disk_reservation_mb(
                memory_mb=4096,
                writable_disk_mb=8192,
            ),
            8192 + 5120 + 4096 + 64,
        )
        self.assertEqual(
            hibernation_disk_reservation_mb(
                memory_mb=4096,
                writable_disk_mb=8192,
                private_pages_mb=64,
            ),
            8192 + 5120 + 64 + 64,
        )

    def test_disk_ledger_reserves_worst_case_and_replays_exactly(self) -> None:
        with TemporaryDirectory() as raw_dir:
            ledger = HibernationDiskLedger(
                (Path(raw_dir) / "disk-ledger.json").resolve(),
                capacity_mb=30_000,
                safety_headroom_mb=2_000,
                first_project_id=300_000,
            )
            reservation = ledger.reserve(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                memory_mb=4096,
                writable_disk_mb=8192,
            )
            self.assertEqual(reservation.project_id, 300_000)
            self.assertEqual(reservation.hibernation_quota_mb, 5120 + 4096 + 64)
            self.assertEqual(reservation.total_mb, 8192 + 5120 + 4096 + 64)
            self.assertEqual(
                ledger.reserve(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                    memory_mb=4096,
                    writable_disk_mb=8192,
                ),
                reservation,
            )
            inventory = ledger.inventory()
            self.assertEqual(inventory.reserved_mb, reservation.total_mb)
            self.assertEqual(
                inventory.available_mb,
                30_000 - 2_000 - reservation.total_mb,
            )

            restarted = HibernationDiskLedger(
                ledger.path,
                capacity_mb=30_000,
                safety_headroom_mb=2_000,
                first_project_id=1,
            )
            self.assertEqual(restarted.inventory(), inventory)

    def test_disk_ledger_rejects_overcommit_and_resource_shape_changes(self) -> None:
        with TemporaryDirectory() as raw_dir:
            ledger = HibernationDiskLedger(
                (Path(raw_dir) / "disk-ledger.json").resolve(),
                capacity_mb=20_000,
                safety_headroom_mb=1_000,
            )
            ledger.reserve(
                sandbox_id="sandbox-1",
                sandbox_generation=1,
                memory_mb=4096,
                writable_disk_mb=4096,
            )
            with self.assertRaisesRegex(
                HibernationConflictError, "different resource bounds"
            ):
                ledger.reserve(
                    sandbox_id="sandbox-1",
                    sandbox_generation=1,
                    memory_mb=4096,
                    writable_disk_mb=4097,
                )
            with self.assertRaisesRegex(HibernationCapacityError, "fail-closed"):
                ledger.reserve(
                    sandbox_id="sandbox-2",
                    sandbox_generation=1,
                    memory_mb=4096,
                    writable_disk_mb=4096,
                )
            self.assertEqual(len(ledger.inventory().reservations), 1)

    def test_disk_ledger_fences_release_and_project_id_reuse(self) -> None:
        with TemporaryDirectory() as raw_dir:
            ledger = HibernationDiskLedger(
                (Path(raw_dir) / "disk-ledger.json").resolve(),
                capacity_mb=40_000,
                safety_headroom_mb=1_000,
                first_project_id=400_000,
            )
            first = ledger.reserve(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                memory_mb=1024,
                writable_disk_mb=2048,
            )
            self.assertEqual(
                ledger.release(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                ),
                first,
            )
            self.assertIsNone(
                ledger.release(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                )
            )
            with self.assertRaisesRegex(HibernationConflictError, "tombstone"):
                ledger.reserve(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                    memory_mb=1024,
                    writable_disk_mb=2048,
                )
            second = ledger.reserve(
                sandbox_id="sandbox-1",
                sandbox_generation=8,
                memory_mb=1024,
                writable_disk_mb=2048,
            )
            self.assertEqual(second.project_id, first.project_id + 1)
            with self.assertRaisesRegex(
                HibernationConflictError, "another sandbox generation"
            ):
                ledger.release(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                )

    def test_disk_ledger_quota_reconciliation_is_exact_and_fail_closed(self) -> None:
        with TemporaryDirectory() as raw_dir:
            ledger = HibernationDiskLedger(
                (Path(raw_dir) / "disk-ledger.json").resolve(),
                capacity_mb=40_000,
                safety_headroom_mb=1_000,
            )
            reservation = ledger.reserve(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                memory_mb=1024,
                writable_disk_mb=2048,
            )
            identity = ("sandbox-1", 7)
            self.assertEqual(
                ledger.require_quota_consistency(
                    expected_incarnations=[identity],
                    project_hard_limits_mb={
                        reservation.project_id: reservation.hibernation_quota_mb,
                    },
                    path_project_ids={identity: reservation.project_id},
                ).reservations,
                (reservation,),
            )
            with self.assertRaisesRegex(HibernationQuotaError, "hard limit"):
                ledger.require_quota_consistency(
                    expected_incarnations=[identity],
                    project_hard_limits_mb={reservation.project_id: 1},
                    path_project_ids={identity: reservation.project_id},
                )
            with self.assertRaisesRegex(HibernationQuotaError, "incarnations"):
                ledger.require_quota_consistency(
                    expected_incarnations=[],
                    project_hard_limits_mb={
                        reservation.project_id: reservation.hibernation_quota_mb,
                    },
                    path_project_ids={identity: reservation.project_id},
                )

    def test_disk_ledger_rejects_corruption_and_capacity_drift(self) -> None:
        with TemporaryDirectory() as raw_dir:
            path = (Path(raw_dir) / "disk-ledger.json").resolve()
            ledger = HibernationDiskLedger(
                path,
                capacity_mb=40_000,
                safety_headroom_mb=1_000,
            )
            ledger.reserve(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                memory_mb=1024,
                writable_disk_mb=2048,
            )
            with self.assertRaisesRegex(HibernationQuotaError, "differs"):
                HibernationDiskLedger(
                    path,
                    capacity_mb=39_999,
                    safety_headroom_mb=1_000,
                ).inventory()
            raw = json.loads(path.read_text(encoding="utf-8"))
            raw["unexpected"] = True
            path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(HibernationQuotaError, "invalid"):
                ledger.inventory()

    def test_json_record_round_trip_is_strict(self) -> None:
        with TemporaryDirectory() as raw_dir:
            journal = HibernationJournal((Path(raw_dir) / "journal.json").resolve())
            record = journal.initialize_running(
                sandbox_id="sandbox-1",
                sandbox_generation=7,
                spec_sha256=DIGEST_B,
                operation_id="create:7",
                sentry_pid=101,
                sentry_start_time_ticks=1001,
            )
            raw = record.to_dict()
            self.assertEqual(
                journal.load(),
                type(record).from_dict(json.loads(json.dumps(raw))),
            )
            raw["unexpected"] = True
            with self.assertRaisesRegex(ValueError, "invalid schema"):
                type(record).from_dict(raw)

    def test_process_identity_rejects_pid_reuse(self) -> None:
        with TemporaryDirectory() as raw_dir:
            proc_root = Path(raw_dir)
            self._write_proc_identity(proc_root, 101, 12345)
            self.assertEqual(
                linux_process_start_time_ticks(101, proc_root=proc_root),
                12345,
            )
            self.assertTrue(
                hibernation_process_identity_matches(
                    101,
                    12345,
                    proc_root=proc_root,
                )
            )
            self.assertFalse(
                hibernation_process_identity_matches(
                    101,
                    54321,
                    proc_root=proc_root,
                )
            )
            self.assertFalse(
                hibernation_process_identity_matches(
                    102,
                    12345,
                    proc_root=proc_root,
                )
            )


if __name__ == "__main__":
    unittest.main()
