import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ucloud_sandboxes.runsc_restore import (
    RESTORE_CHECKPOINT_ANNOTATION,
    RestoreWrapperConfig,
    RestoreWrapperError,
    dispatch,
    render_runsc_restore_script,
)


class RunscRestoreWrapperTests(unittest.TestCase):
    container_id = "a" * 64
    checkpoint_id = "state"

    def _fixture(self, root: Path) -> tuple[RestoreWrapperConfig, Path]:
        runsc = root / "runsc"
        runsc.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        runsc.chmod(0o755)
        docker_root = root / "docker"
        checkpoint_root = docker_root / "ucloud-checkpoints"
        checkpoint_root.mkdir(parents=True)
        staged_root = checkpoint_root / ".staged"
        staged_root.mkdir()
        checkpoint = (
            docker_root
            / "containers"
            / self.container_id
            / "checkpoints"
            / self.checkpoint_id
        )
        checkpoint.mkdir(parents=True)
        marker = {
            "version": 1,
            "state": "staged",
            "artifact_id": "fork-artifact-1",
            "target_container_id": self.container_id,
            "checkpoint_id": self.checkpoint_id,
            "created_ns": 1,
            "updated_ns": 2,
        }
        (staged_root / f"{self.container_id}-{self.checkpoint_id}.json").write_text(
            json.dumps(marker),
            encoding="utf-8",
        )
        config = RestoreWrapperConfig(
            real_runsc=runsc,
            docker_root=docker_root,
            checkpoint_root=checkpoint_root,
            state_root=root / "state",
        )
        return config, checkpoint

    def _bundle(self, root: Path, *, restore: bool = True) -> Path:
        bundle = root / "bundle"
        bundle.mkdir()
        annotations = (
            {RESTORE_CHECKPOINT_ANNOTATION: self.checkpoint_id} if restore else {}
        )
        (bundle / "config.json").write_text(
            json.dumps({"annotations": annotations}),
            encoding="utf-8",
        )
        return bundle

    def test_create_persists_intent_before_raw_restore_start(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir).resolve()
            config, checkpoint = self._fixture(root)
            bundle = self._bundle(root)
            commands: list[tuple[str, ...]] = []

            def run(command):
                commands.append(tuple(command))
                if "create" in command:
                    self.assertTrue(
                        (config.state_root / f"{self.container_id}.json").is_file()
                    )
                return 0

            create = (
                "--root=/run/containerd/runsc",
                "create",
                "--bundle",
                str(bundle),
                self.container_id,
            )
            self.assertEqual(
                dispatch(
                    config,
                    create,
                    run_command=run,
                    require_root_ownership=False,
                ),
                0,
            )
            self.assertEqual(commands[0][1:], create)

            start = ("--root=/run/containerd/runsc", "start", self.container_id)
            self.assertEqual(
                dispatch(
                    config,
                    start,
                    run_command=run,
                    require_root_ownership=False,
                ),
                0,
            )
            restored = commands[1]
            self.assertIn("restore", restored)
            self.assertNotIn("start", restored)
            self.assertIn("--detach", restored)
            self.assertIn(f"--image-path={checkpoint}", restored)
            self.assertFalse(
                (config.state_root / f"{self.container_id}.json").exists()
            )

    def test_regular_create_and_start_delegate_without_restore_intent(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir).resolve()
            config, _checkpoint = self._fixture(root)
            bundle = self._bundle(root, restore=False)
            commands: list[tuple[str, ...]] = []
            def run(command):
                commands.append(tuple(command))
                return 0

            create = ("create", "--bundle", str(bundle), self.container_id)
            start = ("start", self.container_id)
            dispatch(
                config,
                create,
                run_command=run,
                require_root_ownership=False,
            )
            dispatch(
                config,
                start,
                run_command=run,
                require_root_ownership=False,
            )

            self.assertEqual(commands[0][1:], create)
            self.assertEqual(commands[1][1:], start)
            self.assertFalse(config.state_root.joinpath(f"{self.container_id}.json").exists())

    def test_failed_create_removes_intent_and_failed_restore_retains_it(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir).resolve()
            config, _checkpoint = self._fixture(root)
            bundle = self._bundle(root)
            create = ("create", "--bundle", str(bundle), self.container_id)
            intent = config.state_root / f"{self.container_id}.json"

            self.assertEqual(
                dispatch(
                    config,
                    create,
                    run_command=lambda _command: 17,
                    require_root_ownership=False,
                ),
                17,
            )
            self.assertFalse(intent.exists())

            dispatch(
                config,
                create,
                run_command=lambda _command: 0,
                require_root_ownership=False,
            )
            self.assertTrue(intent.exists())
            self.assertEqual(
                dispatch(
                    config,
                    ("start", self.container_id),
                    run_command=lambda _command: 19,
                    require_root_ownership=False,
                ),
                19,
            )
            self.assertTrue(intent.exists())

    def test_delete_clears_ambiguous_restore_intent(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir).resolve()
            config, _checkpoint = self._fixture(root)
            bundle = self._bundle(root)
            dispatch(
                config,
                ("create", "--bundle", str(bundle), self.container_id),
                run_command=lambda _command: 0,
                require_root_ownership=False,
            )
            intent = config.state_root / f"{self.container_id}.json"
            self.assertTrue(intent.exists())

            result = dispatch(
                config,
                ("delete", "--force", self.container_id),
                run_command=lambda _command: 1,
                require_root_ownership=False,
            )

            self.assertEqual(result, 1)
            self.assertFalse(intent.exists())

    def test_restore_requires_helper_staged_marker(self) -> None:
        with TemporaryDirectory() as raw_dir:
            root = Path(raw_dir).resolve()
            config, _checkpoint = self._fixture(root)
            bundle = self._bundle(root)
            marker = (
                config.checkpoint_root
                / ".staged"
                / f"{self.container_id}-{self.checkpoint_id}.json"
            )
            marker.unlink()

            with self.assertRaisesRegex(
                RestoreWrapperError,
                "staged checkpoint marker",
            ):
                dispatch(
                    config,
                    ("create", "--bundle", str(bundle), self.container_id),
                    run_command=lambda _command: 0,
                    require_root_ownership=False,
                )

    def test_rendered_wrapper_embeds_config_path(self) -> None:
        rendered = render_runsc_restore_script(
            config_path="/test/runsc-restore.json"
        )

        self.assertTrue(rendered.startswith("#!/usr/bin/python3\n"))
        self.assertIn(
            "DEFAULT_CONFIG_PATH = '/test/runsc-restore.json'",
            rendered,
        )
        compile(rendered, "<ucloud-runsc-restore>", "exec")


if __name__ == "__main__":
    unittest.main()
