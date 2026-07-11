from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ucloud_sandboxes.checkpoint_helper import (
    ArtifactNotReady,
    CheckpointHelper,
    HelperConfig,
    HelperError,
    _reflink_copy,
    checkpoint_reservation_bytes,
    load_config,
    main,
    render_checkpoint_helper_script,
)


SOURCE_CONTAINER = "a" * 64
TARGET_CONTAINER = "b" * 64
IMAGE_DIGEST = "sha256:" + "c" * 64
SPEC_HASH = "d" * 64


class CheckpointHelperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.docker_root = self.root / "docker"
        self.checkpoint_root = self.docker_root / "ucloud-checkpoints"
        self.containers = self.docker_root / "containers"
        self.checkpoint_root.mkdir(parents=True)
        self.containers.mkdir()
        (self.containers / SOURCE_CONTAINER).mkdir()
        (self.containers / TARGET_CONTAINER).mkdir()
        self.helper = CheckpointHelper(
            HelperConfig(
                docker_root=self.docker_root,
                checkpoint_root=self.checkpoint_root,
            ),
            require_root_ownership=False,
            copy_tree=lambda source, target: shutil.copytree(source, target),
        )

    def _prepare_checkpoint(self, artifact: str = "fork-1") -> Path:
        result = self.helper.prepare(
            artifact,
            SOURCE_CONTAINER,
            IMAGE_DIGEST,
            SPEC_HASH,
            "checkpoint-1",
            1,
            1,
            1,
            1,
        )
        pending = Path(result["pending_path"])
        checkpoint = pending / "checkpoint-1"
        checkpoint.mkdir()
        (checkpoint / "checkpoint.img").write_bytes(b"metadata")
        pages = checkpoint / "pages"
        pages.mkdir()
        (pages / "pages-1.img").write_bytes(b"memory pages")
        return pending

    def test_prepare_seal_status_stage_and_cleanup(self) -> None:
        pending = self._prepare_checkpoint()

        self.helper.complete("fork-1")
        manifest = self.helper.seal("fork-1")

        self.assertFalse(pending.exists())
        self.assertEqual(
            manifest,
            {
                "artifact_id": "fork-1",
                "checkpoint_id": "checkpoint-1",
                "source_container_id": SOURCE_CONTAINER,
                "source_image_id": IMAGE_DIGEST,
                "source_spec_hash": SPEC_HASH,
            },
        )
        self.assertEqual(self.helper.status("fork-1"), manifest)
        artifact = self.checkpoint_root / "fork-1"
        self.assertEqual(
            {entry.name for entry in artifact.iterdir()},
            {"sealed", "manifest.json", ".integrity.json"},
        )

        staged = self.helper.stage("fork-1", TARGET_CONTAINER, "restored")

        self.assertFalse(staged["already_staged"])
        target = self.containers / TARGET_CONTAINER / "checkpoints" / "restored"
        self.assertEqual((target / "checkpoint.img").read_bytes(), b"metadata")
        self.assertTrue(
            self.helper.stage("fork-1", TARGET_CONTAINER, "restored")["already_staged"]
        )
        self.assertTrue(self.helper.unstage(TARGET_CONTAINER, "restored")["removed"])
        self.assertFalse(target.exists())
        self.assertFalse(self.helper.unstage(TARGET_CONTAINER, "restored")["removed"])
        self.assertTrue(self.helper.drop("fork-1")["removed"])
        self.assertFalse(artifact.exists())
        self.assertFalse(self.helper.drop("fork-1")["removed"])

    def test_application_paths_are_generation_scoped_resettable_and_confined(
        self,
    ) -> None:
        first = self.helper.app_prepare("agent-g3-" + "e" * 16)
        path = Path(first["path"])
        self.assertEqual(path.parent, self.checkpoint_root / "application")
        self.assertEqual(path.stat().st_mode & 0o777, 0o700)
        (path / "checkpoint.img").write_bytes(b"stale")

        replay = self.helper.app_prepare("agent-g3-" + "e" * 16)

        self.assertTrue(replay["replaced"])
        self.assertEqual(list(path.iterdir()), [])
        self.assertTrue(self.helper.app_drop("agent-g3-" + "e" * 16)["removed"])
        self.assertFalse(path.exists())
        self.assertFalse(self.helper.app_drop("agent-g3-" + "e" * 16)["removed"])
        with self.assertRaises(HelperError):
            self.helper.app_prepare("../escape")

    def test_list_reports_validated_artifacts_applications_and_staged_refs(
        self,
    ) -> None:
        self.helper.app_prepare("parent-g1-" + "a" * 16)
        self._prepare_checkpoint()
        pending_list = self.helper.list_state()
        self.assertEqual(
            pending_list["artifacts"],
            [
                {
                    "artifact_id": "fork-1",
                    "state": "pending",
                    "source_container_id": SOURCE_CONTAINER,
                    "checkpoint_id": "checkpoint-1",
                }
            ],
        )
        self.helper.complete("fork-1")
        self.helper.seal("fork-1")
        self.helper.stage("fork-1", TARGET_CONTAINER, "restored")

        state = self.helper.list_state()

        self.assertEqual(state["version"], 1)
        self.assertEqual(state["applications"], ["parent-g1-" + "a" * 16])
        self.assertEqual(state["artifacts"][0]["state"], "sealed")
        self.assertEqual(len(state["staged"]), 1)
        self.assertEqual(state["staged"][0]["artifact_id"], "fork-1")
        self.assertEqual(state["staged"][0]["state"], "staged")
        self.assertTrue(state["staged"][0]["target_present"])
        self.assertTrue(state["staged"][0]["content_matches"])
        with self.assertRaisesRegex(HelperError, "staged reference"):
            self.helper.drop("fork-1")
        self.helper.unstage(TARGET_CONTAINER, "restored")
        self.assertTrue(self.helper.drop("fork-1")["removed"])

    def test_reserved_application_name_cannot_be_used_as_artifact(self) -> None:
        with self.assertRaisesRegex(HelperError, "reserved"):
            self.helper.prepare(
                "application",
                SOURCE_CONTAINER,
                IMAGE_DIGEST,
                SPEC_HASH,
                "checkpoint-1",
                1,
                1,
                1,
                1,
            )

    def test_status_is_not_ready_until_checkpoint_is_sealed(self) -> None:
        self.helper.prepare(
            "fork-1",
            SOURCE_CONTAINER,
            IMAGE_DIGEST,
            SPEC_HASH,
            "checkpoint-1",
            1,
            1,
            1,
            1,
        )

        with self.assertRaises(ArtifactNotReady):
            self.helper.status("fork-1")

    def test_prepare_fails_closed_without_bounded_free_space(self) -> None:
        reservation = checkpoint_reservation_bytes(128, 256, 64, 16)
        self.assertEqual(
            reservation,
            (2 * 128 + 256 + 64 + 16 + 64) * 1024 * 1024,
        )
        filesystem = SimpleNamespace(
            f_frsize=1,
            f_bsize=1,
            f_bavail=reservation - 1,
        )

        with patch(
            "ucloud_sandboxes.checkpoint_helper.os.statvfs",
            return_value=filesystem,
        ), self.assertRaisesRegex(HelperError, "insufficient checkpoint storage"):
            self.helper.prepare(
                "fork-capacity",
                SOURCE_CONTAINER,
                IMAGE_DIGEST,
                SPEC_HASH,
                "checkpoint-1",
                128,
                256,
                64,
                16,
            )

        self.assertFalse((self.checkpoint_root / "fork-capacity").exists())

    def test_prepare_fails_closed_when_capacity_cannot_be_measured(self) -> None:
        with patch(
            "ucloud_sandboxes.checkpoint_helper.os.statvfs",
            side_effect=OSError("unavailable"),
        ), self.assertRaisesRegex(HelperError, "refusing prepare"):
            self.helper.prepare(
                "fork-statvfs",
                SOURCE_CONTAINER,
                IMAGE_DIGEST,
                SPEC_HASH,
                "checkpoint-1",
                1,
                1,
                1,
                1,
            )

        self.assertFalse((self.checkpoint_root / "fork-statvfs").exists())

    def test_prepare_accounts_for_all_pending_reservations(self) -> None:
        reservation = checkpoint_reservation_bytes(1, 1, 1, 1)
        self.helper.prepare(
            "fork-pending-a",
            SOURCE_CONTAINER,
            IMAGE_DIGEST,
            SPEC_HASH,
            "checkpoint-1",
            1,
            1,
            1,
            1,
        )
        filesystem = SimpleNamespace(
            f_frsize=1,
            f_bsize=1,
            f_bavail=reservation * 2 - 1,
        )

        with patch(
            "ucloud_sandboxes.checkpoint_helper.os.statvfs",
            return_value=filesystem,
        ), self.assertRaisesRegex(
            HelperError,
            f"pending_reservations={reservation}",
        ):
            self.helper.prepare(
                "fork-pending-b",
                SOURCE_CONTAINER,
                IMAGE_DIGEST,
                SPEC_HASH,
                "checkpoint-1",
                1,
                1,
                1,
                1,
            )

    def test_seal_rejects_checkpoint_larger_than_declared_bound(self) -> None:
        pending = self._prepare_checkpoint("fork-oversize")
        oversized = pending / "checkpoint-1" / "oversized.img"
        with oversized.open("wb") as handle:
            handle.truncate(checkpoint_reservation_bytes(1, 1, 1, 1) + 1)
        self.helper.complete("fork-oversize")

        with self.assertRaisesRegex(HelperError, "exceeds its storage reservation"):
            self.helper.seal("fork-oversize")

    def test_gc_removes_only_exact_helper_owned_crash_debris(self) -> None:
        self.helper.prepare(
            "fork-pending",
            SOURCE_CONTAINER,
            IMAGE_DIGEST,
            SPEC_HASH,
            "checkpoint-1",
            1,
            1,
            1,
            1,
        )
        suffix = "a" * 32
        root_prepare = self.checkpoint_root / f".prepare-orphan-{suffix}"
        root_drop = self.checkpoint_root / f".ucloud-drop-orphan-{suffix}"
        root_prepare.mkdir()
        root_drop.mkdir()
        lookalike = self.checkpoint_root / ".prepare-not-helper-owned"
        lookalike.mkdir()
        manifest_temp = (
            self.checkpoint_root / "fork-pending" / f"..complete.json.tmp-{suffix}"
        )
        manifest_temp.write_text("partial", encoding="utf-8")
        checkpoints = self.containers / TARGET_CONTAINER / "checkpoints"
        checkpoints.mkdir()
        stage_temp = checkpoints / f".ucloud-stage-state-{suffix}"
        unstage_temp = checkpoints / f".ucloud-unstage-state-{suffix}"
        stage_temp.mkdir()
        unstage_temp.mkdir()

        result = self.helper.gc()

        self.assertEqual(result["removed_root_temps"], 2)
        self.assertEqual(result["removed_checkpoint_temps"], 2)
        self.assertEqual(result["removed_manifest_temps"], 1)
        self.assertFalse(root_prepare.exists())
        self.assertFalse(root_drop.exists())
        self.assertFalse(stage_temp.exists())
        self.assertFalse(unstage_temp.exists())
        self.assertFalse(manifest_temp.exists())
        self.assertTrue(lookalike.exists())
        self.assertTrue((self.checkpoint_root / "fork-pending").exists())
        with self.assertRaises(ArtifactNotReady):
            self.helper.status("fork-pending")

    def test_gc_refuses_to_follow_exactly_named_symlink(self) -> None:
        outside = self.root / "outside"
        outside.mkdir()
        marker = outside / "keep"
        marker.write_text("safe", encoding="utf-8")
        trash = self.checkpoint_root / (".ucloud-drop-orphan-" + "b" * 32)
        trash.symlink_to(outside, target_is_directory=True)

        with self.assertRaisesRegex(HelperError, "real directory"):
            self.helper.gc()

        self.assertEqual(marker.read_text(encoding="utf-8"), "safe")

    def test_seal_requires_durable_docker_completion_marker(self) -> None:
        self._prepare_checkpoint()

        with self.assertRaisesRegex(HelperError, "complete.json"):
            self.helper.seal("fork-1")

        completion = self.helper.complete("fork-1")
        self.assertEqual(completion["state"], "complete")
        self.assertEqual(self.helper.complete("fork-1"), completion)
        self.assertEqual(self.helper.seal("fork-1")["artifact_id"], "fork-1")

    def test_main_distinguishes_missing_from_unsealed_artifact(self) -> None:
        config = self.root / "helper.json"
        config.write_text(
            json.dumps(
                {
                    "version": 1,
                    "docker_root": str(self.docker_root),
                    "checkpoint_root": str(self.checkpoint_root),
                }
            ),
            encoding="utf-8",
        )
        stderr = io.StringIO()
        stdout = io.StringIO()

        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = main(
                ["status", "missing"],
                config_path=config,
                require_root=False,
            )

        self.assertEqual(result, 4)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("does not exist", stderr.getvalue())

        self.helper.prepare(
            "fork-1",
            SOURCE_CONTAINER,
            IMAGE_DIGEST,
            SPEC_HASH,
            "checkpoint-1",
            1,
            1,
            1,
            1,
        )
        stderr = io.StringIO()
        with redirect_stdout(io.StringIO()), redirect_stderr(stderr):
            result = main(
                ["status", "fork-1"],
                config_path=config,
                require_root=False,
            )
        self.assertEqual(result, 3)
        self.assertIn("not sealed", stderr.getvalue())

    def test_rejects_unsafe_identifiers_and_checkpoint_symlinks(self) -> None:
        for artifact in ("../escape", ".", "", "with/slash"):
            with self.subTest(artifact=artifact), self.assertRaises(HelperError):
                self.helper.prepare(
                    artifact,
                    SOURCE_CONTAINER,
                    IMAGE_DIGEST,
                    SPEC_HASH,
                    "checkpoint-1",
                    1,
                    1,
                    1,
                    1,
                )

        pending = self._prepare_checkpoint("fork-symlink")
        (pending / "checkpoint-1" / "escape").symlink_to(self.root)
        self.helper.complete("fork-symlink")
        with self.assertRaisesRegex(HelperError, "unsupported entry"):
            self.helper.seal("fork-symlink")

    def test_detects_manifest_or_checkpoint_tampering(self) -> None:
        self._prepare_checkpoint()
        self.helper.complete("fork-1")
        self.helper.seal("fork-1")
        artifact = self.checkpoint_root / "fork-1"
        manifest_path = artifact / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["unexpected"] = True
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(HelperError, "invalid schema"):
            self.helper.status("fork-1")

        del manifest["unexpected"]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        checkpoint = artifact / "sealed" / "checkpoint-1" / "checkpoint.img"
        checkpoint.write_bytes(b"changed size")
        with self.assertRaisesRegex(HelperError, "does not match"):
            self.helper.status("fork-1")

    def test_config_rejects_checkpoint_root_outside_docker_root(self) -> None:
        config = self.root / "helper.json"
        config.write_text(
            json.dumps(
                {
                    "version": 1,
                    "docker_root": str(self.docker_root),
                    "checkpoint_root": str(self.root / "elsewhere"),
                }
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(HelperError, "DockerRootDir"):
            load_config(config, require_root_ownership=False)

    def test_rendered_helper_is_standalone_and_uses_fixed_config(self) -> None:
        rendered = render_checkpoint_helper_script(config_path="/fixed/helper.json")

        self.assertTrue(rendered.startswith("#!/usr/bin/python3\n"))
        self.assertIn("DEFAULT_CONFIG_PATH = '/fixed/helper.json'", rendered)
        compile(rendered, "<checkpoint-helper>", "exec")

    @patch("ucloud_sandboxes.checkpoint_helper.subprocess.run")
    def test_reflink_copy_has_no_byte_copy_fallback(self, run) -> None:
        run.return_value = subprocess.CompletedProcess([], 0)

        _reflink_copy(Path("/source"), Path("/target"))

        command = run.call_args.args[0]
        self.assertEqual(command[0], "/bin/cp")
        self.assertIn("--reflink=always", command)
        self.assertIn("--no-target-directory", command)


if __name__ == "__main__":
    unittest.main()
