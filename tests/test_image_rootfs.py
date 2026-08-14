import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread
from time import monotonic, sleep
import unittest

from ucloud_sandboxes.direct_warden import CommandResult, DirectWardenError
from ucloud_sandboxes.image_rootfs import (
    DockerOverlay2RootfsStore,
    OverlayRootfsManager,
)


IMAGE_DIGEST = "a" * 64
OTHER_IMAGE_DIGEST = "b" * 64


class FakeRunner:
    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.mounted: set[str] = set()
        self.not_mounted_returncode = 1

    def run(self, argv, *, timeout):
        del timeout
        command = tuple(str(item) for item in argv)
        self.commands.append(command)
        if command[0] == "mount":
            self.mounted.add(command[-1])
        elif command[0] == "mountpoint":
            return CommandResult(
                command,
                (0 if command[-1] in self.mounted else self.not_mounted_returncode),
            )
        elif command[0] == "umount":
            self.mounted.discard(command[-1])
        return CommandResult(command, 0)


class Overlay2Runner(FakeRunner):
    def __init__(self, docker_root: Path, *, single_layer: bool = False) -> None:
        super().__init__()
        self.docker_root = docker_root.resolve()
        self.single_layer = single_layer
        self.pins: dict[str, str] = {}
        self.fail_umount = False
        self.top = self.docker_root / "overlay2" / "top" / "diff"
        self.middle = self.docker_root / "overlay2" / "middle" / "diff"
        self.base = self.docker_root / "overlay2" / "base" / "diff"
        for path in (self.top, self.middle, self.base):
            path.mkdir(parents=True)

    def run(self, argv, *, timeout):
        command = tuple(str(item) for item in argv)
        if command[:3] == ("docker", "image", "inspect"):
            self.commands.append(command)
            return CommandResult(
                command,
                0,
                json.dumps(
                    [
                        {
                            "Id": f"sha256:{IMAGE_DIGEST}",
                            "Config": {
                                "Cmd": ["true"],
                                "WorkingDir": "/workspace",
                            },
                            "GraphDriver": {
                                "Name": "overlay2",
                                "Data": {
                                    "UpperDir": str(self.top),
                                    "LowerDir": (
                                        ""
                                        if self.single_layer
                                        else f"{self.middle}:{self.base}"
                                    ),
                                },
                            },
                        }
                    ]
                ),
            )
        if command == ("docker", "info", "--format={{json .Driver}}"):
            self.commands.append(command)
            return CommandResult(command, 0, json.dumps("overlay2"))
        if command[:3] == ("docker", "image", "tag"):
            self.commands.append(command)
            repository, digest = command[4].rsplit(":", 1)
            if repository == DockerOverlay2RootfsStore.PIN_REPOSITORY:
                self.pins[digest] = command[3]
            return CommandResult(command, 0)
        if command[:3] == ("docker", "image", "rm"):
            self.commands.append(command)
            repository, digest = command[3].rsplit(":", 1)
            if repository == DockerOverlay2RootfsStore.PIN_REPOSITORY:
                self.pins.pop(digest, None)
            return CommandResult(command, 0)
        if command[:3] == ("docker", "image", "ls"):
            self.commands.append(command)
            return CommandResult(
                command,
                0,
                "\n".join(
                    f"{DockerOverlay2RootfsStore.PIN_REPOSITORY} {digest} {image_id}"
                    for digest, image_id in sorted(self.pins.items())
                ),
            )
        if command[0] == "umount" and self.fail_umount:
            self.commands.append(command)
            return CommandResult(command, 32, stderr="injected unmount failure")
        return super().run(command, timeout=timeout)


class MultiImageBlockingMountRunner(Overlay2Runner):
    def __init__(self, docker_root: Path) -> None:
        super().__init__(docker_root)
        self.other_top = self.docker_root / "overlay2" / "other-top" / "diff"
        self.other_middle = self.docker_root / "overlay2" / "other-middle" / "diff"
        self.other_base = self.docker_root / "overlay2" / "other-base" / "diff"
        for path in (self.other_top, self.other_middle, self.other_base):
            path.mkdir(parents=True)
        self.first_mount_started = Event()
        self.first_mount_release = Event()

    def run(self, argv, *, timeout):
        command = tuple(str(item) for item in argv)
        if command[:3] == ("docker", "image", "inspect"):
            self.commands.append(command)
            other = command[3] == "example/other:latest"
            digest = OTHER_IMAGE_DIGEST if other else IMAGE_DIGEST
            top = self.other_top if other else self.top
            middle = self.other_middle if other else self.middle
            base = self.other_base if other else self.base
            return CommandResult(
                command,
                0,
                json.dumps(
                    [
                        {
                            "Id": f"sha256:{digest}",
                            "Config": {"Cmd": ["true"]},
                            "GraphDriver": {
                                "Name": "overlay2",
                                "Data": {
                                    "UpperDir": str(top),
                                    "LowerDir": f"{middle}:{base}",
                                },
                            },
                        }
                    ]
                ),
            )
        if command[0] == "mount" and command[-1].endswith(f"/{IMAGE_DIGEST}/rootfs"):
            self.commands.append(command)
            self.first_mount_started.set()
            if not self.first_mount_release.wait(timeout=5):
                raise AssertionError("test did not release first image mount")
            self.mounted.add(command[-1])
            return CommandResult(command, 0)
        return super().run(command, timeout=timeout)


def image_store(root: Path, runner: Overlay2Runner) -> DockerOverlay2RootfsStore:
    return DockerOverlay2RootfsStore(
        (root / "cache").resolve(),
        runner=runner,
        docker_root=runner.docker_root,
    )


class ImageRootfsTests(unittest.TestCase):
    def test_overlay2_store_mounts_shared_layers_without_export(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = Overlay2Runner(root / "docker")
            store = image_store(root, runner)

            with store.operation_lease("example/image:latest") as first:
                pass
            with store.operation_lease("example/image:latest") as second:
                pass

            self.assertEqual(first, second)
            self.assertEqual(first.image_config.command, ("true",))
            self.assertEqual(
                sum(command[0] == "mount" for command in runner.commands),
                1,
            )
            self.assertFalse(
                any(
                    command[1:2] in (("create",), ("export",))
                    for command in runner.commands
                )
            )

    def test_overlay2_startup_reconciles_durable_image_lease(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            docker_root = root / "docker"
            runner = Overlay2Runner(docker_root)
            store = image_store(root, runner)
            with store.operation_lease("example/image:latest") as materialized:
                pass
            runner.commands.clear()
            runner.pins.clear()

            store.reconcile_images(
                (materialized.image_id,),
                is_referenced=lambda image_id: image_id == materialized.image_id,
            )

            self.assertIn(
                (
                    "docker",
                    "image",
                    "tag",
                    f"sha256:{IMAGE_DIGEST}",
                    f"ucloud-sandbox-rootfs-cache:{IMAGE_DIGEST}",
                ),
                runner.commands,
            )

    def test_overlay2_gc_evicts_only_unreferenced_cache_and_private_pin(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = Overlay2Runner(root / "docker")
            store = image_store(root, runner)
            with store.operation_lease("example/image:latest") as materialized:
                pass
            target = materialized.rootfs.parent

            result = store.reconcile_images((), is_referenced=lambda _image_id: False)

            self.assertEqual(result["collected"], 1)
            self.assertFalse(target.exists())
            self.assertNotIn(str(materialized.rootfs), runner.mounted)
            self.assertNotIn(IMAGE_DIGEST, runner.pins)

    def test_overlay2_gc_waits_for_digest_lease_and_rechecks_registry_root(
        self,
    ) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = Overlay2Runner(root / "docker")
            store = image_store(root, runner)
            with store.operation_lease("example/image:latest") as materialized:
                pass
            lease_acquired = Event()
            release_lease = Event()
            committed = Event()
            gc_done = Event()
            errors: list[BaseException] = []

            def hold_lease() -> None:
                try:
                    with store.operation_lease("example/image:latest"):
                        lease_acquired.set()
                        if not release_lease.wait(timeout=5):
                            raise AssertionError("test did not release image lease")
                except BaseException as exc:
                    errors.append(exc)

            def collect() -> None:
                try:
                    store.reconcile_images(
                        (),
                        is_referenced=lambda _image_id: committed.is_set(),
                    )
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    gc_done.set()

            holder = Thread(target=hold_lease)
            holder.start()
            self.assertTrue(lease_acquired.wait(timeout=2))
            collector = Thread(target=collect)
            collector.start()
            self.assertFalse(gc_done.wait(timeout=0.1))

            committed.set()
            release_lease.set()
            holder.join(timeout=5)
            collector.join(timeout=5)

            self.assertFalse(holder.is_alive())
            self.assertFalse(collector.is_alive())
            self.assertEqual(errors, [])
            self.assertTrue(materialized.rootfs.parent.exists())
            self.assertIn(IMAGE_DIGEST, runner.pins)

    def test_overlay2_gc_fails_closed_when_cache_mount_cannot_unmount(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = Overlay2Runner(root / "docker")
            store = image_store(root, runner)
            with store.operation_lease("example/image:latest") as materialized:
                pass
            runner.fail_umount = True

            with self.assertRaisesRegex(DirectWardenError, "could not discard"):
                store.reconcile_images((), is_referenced=lambda _image_id: False)

            self.assertTrue(materialized.rootfs.parent.exists())
            self.assertIn(str(materialized.rootfs), runner.mounted)
            self.assertIn(IMAGE_DIGEST, runner.pins)

    def test_same_digest_waiters_do_not_starve_an_unrelated_image(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = MultiImageBlockingMountRunner(root / "docker")
            store = DockerOverlay2RootfsStore(
                (root / "cache").resolve(),
                runner=runner,
                docker_root=runner.docker_root,
                max_concurrent_operations=2,
            )
            other_acquired = Event()
            errors: list[BaseException] = []

            def lease(image_ref: str, acquired: Event | None = None) -> None:
                try:
                    with store.operation_lease(image_ref):
                        if acquired is not None:
                            acquired.set()
                except BaseException as exc:
                    errors.append(exc)

            first = Thread(target=lease, args=("example/image:latest",))
            duplicate = Thread(target=lease, args=("example/image:latest",))
            unrelated = Thread(
                target=lease,
                args=("example/other:latest", other_acquired),
            )
            first.start()
            self.assertTrue(runner.first_mount_started.wait(timeout=2))
            duplicate.start()
            deadline = monotonic() + 2
            snapshot = store.operation_snapshot()
            while snapshot["waiting_operations"] < 1 and monotonic() < deadline:
                sleep(0.01)
                snapshot = store.operation_snapshot()

            self.assertEqual(snapshot["active_operations"], 1)
            self.assertGreaterEqual(snapshot["waiting_operations"], 1)
            unrelated.start()
            progressed = other_acquired.wait(timeout=2)
            runner.first_mount_release.set()
            first.join(timeout=5)
            duplicate.join(timeout=5)
            unrelated.join(timeout=5)

            self.assertTrue(progressed)
            self.assertFalse(first.is_alive())
            self.assertFalse(duplicate.is_alive())
            self.assertFalse(unrelated.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(
                store.operation_snapshot(),
                {
                    "active_operations": 0,
                    "waiting_operations": 0,
                    "max_concurrent_operations": 2,
                },
            )

    def test_cold_same_digest_callers_share_the_provisioning_lease(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = Overlay2Runner(root / "docker")
            store = image_store(root, runner)
            first_acquired = Event()
            second_acquired = Event()
            release_first = Event()
            errors: list[BaseException] = []

            def first() -> None:
                try:
                    with store.operation_lease("example/image:latest"):
                        first_acquired.set()
                        if not release_first.wait(timeout=5):
                            raise AssertionError("test did not release first caller")
                except BaseException as exc:
                    errors.append(exc)

            def second() -> None:
                try:
                    with store.operation_lease("example/image:latest"):
                        second_acquired.set()
                except BaseException as exc:
                    errors.append(exc)

            first_thread = Thread(target=first)
            second_thread = Thread(target=second)
            first_thread.start()
            self.assertTrue(first_acquired.wait(timeout=2))
            second_thread.start()
            shared = second_acquired.wait(timeout=2)
            release_first.set()
            first_thread.join(timeout=5)
            second_thread.join(timeout=5)

            self.assertTrue(shared)
            self.assertFalse(first_thread.is_alive())
            self.assertFalse(second_thread.is_alive())
            self.assertEqual(errors, [])

    def test_overlay2_store_rejects_layers_outside_docker_root(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            docker_root = root / "docker"
            runner = Overlay2Runner(docker_root)
            escaped = root / "escaped"
            escaped.mkdir()
            runner.top = escaped
            store = DockerOverlay2RootfsStore(
                (root / "cache").resolve(),
                runner=runner,
                docker_root=docker_root.resolve(),
            )

            with self.assertRaisesRegex(DirectWardenError, "escaped"):
                with store.operation_lease("example/image:latest"):
                    pass

    def test_overlay_bundle_uses_shared_lower_and_per_sandbox_upper(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = Overlay2Runner(root / "docker")
            store = image_store(root, runner)
            manager = OverlayRootfsManager(
                store,
                writable_root=(root / "writable").resolve(),
                bundle_root=(root / "bundles").resolve(),
                runner=runner,
            )

            with store.operation_lease("example/image:latest") as image:
                lease = manager.prepare(
                    sandbox_id="sandbox-1",
                    sandbox_generation=7,
                    image=image,
                    config_template={"root": {"path": "unused", "readonly": True}},
                )

            config = json.loads(
                (lease.sandbox.bundle / "config.json").read_text(encoding="utf-8")
            )
            self.assertEqual(config["root"]["path"], "rootfs")
            self.assertTrue(config["root"]["readonly"])
            self.assertEqual(
                lease.sandbox.rootfs_sha256,
                lease.image.rootfs_identity_sha256,
            )
            self.assertTrue(lease.writable_owned_by_manager)
            mount = next(
                command
                for command in runner.commands
                if command[0] == "mount" and "upperdir=" in " ".join(command)
            )
            options = mount[mount.index("-o") + 1]
            self.assertIn(f"lowerdir={lease.image.rootfs}", options)
            self.assertIn(f"upperdir={lease.upper}", options)
            self.assertEqual(
                lease.upper.stat().st_mode & 0o7777,
                lease.image.rootfs.stat().st_mode & 0o7777,
            )

            manager.park_sandbox(lease.sandbox)
            self.assertNotIn(str(lease.merged), runner.mounted)
            runner.not_mounted_returncode = 32
            manager.resume_sandbox(lease.sandbox)
            self.assertIn(str(lease.merged), runner.mounted)
            mounts = [command for command in runner.commands if command[0] == "mount"]
            self.assertEqual(len(mounts), 3)

            manager.release(lease)
            self.assertFalse(lease.sandbox.bundle.exists())
            self.assertFalse(lease.upper.parent.exists())

    def test_overlay_prepare_unmounts_and_removes_partial_state(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = Overlay2Runner(root / "docker")
            store = image_store(root, runner)
            manager = OverlayRootfsManager(
                store,
                writable_root=(root / "writable").resolve(),
                bundle_root=(root / "bundles").resolve(),
                runner=runner,
            )
            recursive: dict[str, object] = {}
            recursive["recursive"] = recursive

            with store.operation_lease("example/image:latest") as image:
                with self.assertRaises(ValueError):
                    manager.prepare(
                        sandbox_id="sandbox-1",
                        sandbox_generation=8,
                        image=image,
                        config_template=recursive,
                    )

            self.assertTrue(any(command[0] == "umount" for command in runner.commands))
            self.assertEqual(list(manager.bundle_root.iterdir()), [])
            self.assertEqual(list(manager.writable_root.iterdir()), [])

    def test_quota_owned_overlay_requires_and_preserves_precreated_root(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = Overlay2Runner(root / "docker")
            writable_root = (root / "quota").resolve()
            writable_root.mkdir()
            incarnation = writable_root / "sandbox-1.sandbox-9"
            incarnation.mkdir(mode=0o700)
            store = image_store(root, runner)
            manager = OverlayRootfsManager(
                store,
                writable_root=writable_root,
                bundle_root=(root / "bundles").resolve(),
                runner=runner,
                require_precreated_writable=True,
            )

            with store.operation_lease("example/image:latest") as image:
                lease = manager.prepare(
                    sandbox_id="sandbox-1",
                    sandbox_generation=9,
                    image=image,
                    config_template={"root": {}},
                )
            self.assertFalse(lease.writable_owned_by_manager)
            self.assertEqual(lease.writable, incarnation)
            self.assertEqual(lease.sandbox.memory_directory, incarnation.name)

            manager.release(lease)
            self.assertTrue(incarnation.is_dir())
            self.assertTrue((incarnation / "upper").is_dir())
            self.assertTrue((incarnation / "work").is_dir())

    def test_imported_overlay_preserves_upper_and_parked_generation(self) -> None:
        with TemporaryDirectory() as raw:
            root = Path(raw)
            runner = Overlay2Runner(root / "docker")
            writable_root = (root / "quota").resolve()
            incarnation = writable_root / "sandbox-1.sandbox-10"
            upper = incarnation / "upper"
            generation = incarnation / "hibernate-3"
            upper.mkdir(parents=True)
            generation.mkdir()
            (upper / "payload").write_bytes(b"migrated")
            (generation / "checkpoint.img").write_bytes(b"checkpoint")
            store = image_store(root, runner)
            manager = OverlayRootfsManager(
                store,
                writable_root=writable_root,
                bundle_root=(root / "bundles").resolve(),
                runner=runner,
                require_precreated_writable=True,
            )

            with store.operation_lease("example/image:latest") as image:
                lease = manager.prepare(
                    sandbox_id="sandbox-1",
                    sandbox_generation=10,
                    image=image,
                    config_template={"root": {}},
                    imported_parked=True,
                )

            self.assertEqual((lease.upper / "payload").read_bytes(), b"migrated")
            self.assertTrue((generation / "checkpoint.img").is_file())
            self.assertTrue(lease.work.is_dir())


if __name__ == "__main__":
    unittest.main()
