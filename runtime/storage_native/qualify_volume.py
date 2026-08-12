#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
from dataclasses import asdict, dataclass
import errno
import json
import mmap
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any, Iterator


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT))

from ucloud_sandboxes.storage_native import (  # noqa: E402
    AgentEnvUblkClient,
    StorageNativeDevice,
)


MIB = 1024 * 1024
GIB = 1024 * MIB
MEMORY_SIZE = 256 * MIB
HOLE_OFFSET = 128 * MIB
HOLE_SIZE = 1 * MIB
SENTINELS = {
    0: b"ucloud-memory-start",
    64 * MIB: b"ucloud-memory-middle",
    MEMORY_SIZE - 4096: b"ucloud-memory-end",
}


@dataclass(frozen=True)
class Counters:
    daemon_read_bytes: int
    daemon_write_bytes: int
    daemon_user_ticks: int
    daemon_system_ticks: int
    device_read_sectors: int
    device_write_sectors: int

    def minus(self, before: "Counters") -> dict[str, int]:
        return {
            field: getattr(self, field) - getattr(before, field)
            for field in self.__dataclass_fields__
        }


class Qualifier:
    def __init__(
        self,
        *,
        daemon_binary: Path,
        work_root: Path,
        output: Path,
        virtual_size: int,
        upper_mode: str,
        runsc: Path | None = None,
        conformance_workload: Path | None = None,
        noop_workload: Path | None = None,
    ) -> None:
        self.daemon_binary = daemon_binary
        self.work_root = work_root
        self.output = output
        self.virtual_size = virtual_size
        self.upper_mode = upper_mode
        self.runsc = runsc
        self.conformance_workload = conformance_workload
        self.noop_workload = noop_workload
        self.test_root: Path | None = None
        self.daemon: subprocess.Popen[str] | None = None
        self.client: AgentEnvUblkClient | None = None
        self.devices: dict[int, StorageNativeDevice] = {}
        self.mounts: list[Path] = []
        self.phases: dict[str, dict[str, Any]] = {}
        self.result: dict[str, Any] = {
            "schema": 2,
            "status": "failed",
            "upper_mode": upper_mode,
            "virtual_size": virtual_size,
        }

    def run(self) -> dict[str, Any]:
        self._preflight()
        assert self.test_root is not None
        try:
            with self._phase("daemon_start"):
                self._start_daemon()
            with self._phase("create_and_format"):
                initial = self._create_initial_device()
                self._command("mkfs.ext4", "-F", "-m", "0", str(initial.device_path))
            with self._phase("mount_and_populate", initial):
                initial_mount = self.test_root / "initial-mount"
                initial_mount.mkdir()
                self._mount(initial.device_path, initial_mount, "-o", "noatime")
                self._populate_and_verify_initial(initial_mount)
                if self.runsc is not None:
                    self._gvisor_create_and_park(initial_mount)
            with self._phase("freeze_and_seal", initial):
                layer = self._freeze_and_seal(initial, initial_mount)
            with self._phase("release_initial"):
                self._unmount(self.test_root / "overlay-merged")
                self._unmount(initial_mount)
                self._delete_device(initial)
            with self._phase("reconstruct_and_mount"):
                resumed = self._create_resumed_device(layer)
                resumed_mount = self.test_root / "resumed-mount"
                resumed_mount.mkdir()
                self._mount(resumed.device_path, resumed_mount, "-o", "noatime")
            with self._phase("verify_reconstructed", resumed):
                self._verify_reconstructed(resumed_mount)
                if self.runsc is not None:
                    self._gvisor_restore_and_verify(resumed, resumed_mount)
            with self._phase("enospc", resumed):
                self._verify_enospc(resumed_mount)
            with self._phase("release_resumed"):
                self._unmount(resumed_mount)
                self._delete_device(resumed)

            snapshot_path = self.test_root / "layers" / "generation-1.commit"
            snapshot_stat = snapshot_path.stat()
            allocated = snapshot_stat.st_blocks * 512
            self.result["snapshot"] = {
                "allocated_bytes": allocated,
                "file_bytes": snapshot_stat.st_size,
                "logical_device_bytes": self.virtual_size,
                "allocated_to_logical_ratio": allocated / self.virtual_size,
                "file_to_logical_ratio": snapshot_stat.st_size / self.virtual_size,
            }
            if allocated >= self.virtual_size // 2:
                raise RuntimeError(
                    "sealed layer allocated at least half of the logical device"
                )
            self.result["status"] = "passed"
            return self.result
        except BaseException as exc:
            self.result["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            self.result["phases"] = self.phases
            self._cleanup()
            self.output.parent.mkdir(parents=True, exist_ok=True)
            self.output.write_text(
                json.dumps(self.result, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

    def _preflight(self) -> None:
        if sys.platform != "linux":
            raise RuntimeError("storage-native qualification requires Linux")
        if os.geteuid() != 0:
            raise RuntimeError("storage-native qualification must run as root")
        if not self.daemon_binary.is_absolute() or not self.daemon_binary.is_file():
            raise ValueError("--daemon must be an absolute path to a binary")
        if not os.access(self.daemon_binary, os.X_OK):
            raise ValueError("--daemon is not executable")
        if not self.work_root.is_absolute() or not self.work_root.is_dir():
            raise ValueError("--work-root must be an existing absolute directory")
        if self.work_root == Path("/"):
            raise ValueError("--work-root may not be the filesystem root")
        if not self.output.is_absolute():
            raise ValueError("--output must be absolute")
        if self.virtual_size < GIB or self.virtual_size % GIB:
            raise ValueError("--size-gib must produce a whole-GiB device")
        if self.upper_mode not in {
            "sparse",
            "logStructured",
            "hybridLogStructured",
        }:
            raise ValueError("unsupported overlaybd upper mode")
        gvisor_inputs = (
            self.runsc,
            self.conformance_workload,
            self.noop_workload,
        )
        if any(gvisor_inputs) and not all(gvisor_inputs):
            raise ValueError(
                "--runsc, --conformance-workload, and --noop-workload "
                "must be supplied together"
            )
        for label, path in (
            ("--runsc", self.runsc),
            ("--conformance-workload", self.conformance_workload),
            ("--noop-workload", self.noop_workload),
        ):
            if path is not None and (
                not path.is_absolute()
                or not path.is_file()
                or not os.access(path, os.X_OK)
            ):
                raise ValueError(f"{label} must be an absolute executable file")
        for tool in (
            "fallocate",
            "fsfreeze",
            "getfacl",
            "mkfs.ext4",
            "mount",
            "setfacl",
            "umount",
        ):
            if shutil.which(tool) is None:
                raise RuntimeError(f"required host tool is missing: {tool}")
        if self.runsc is not None and shutil.which("ip") is None:
            raise RuntimeError("required gVisor host tool is missing: ip")
        if not Path("/dev/ublk-control").exists():
            raise RuntimeError("/dev/ublk-control is unavailable")
        raw_root = tempfile.mkdtemp(
            prefix="ucloud-storage-native-",
            dir=self.work_root,
        )
        self.test_root = Path(raw_root).resolve()
        if self.test_root.parent != self.work_root.resolve():
            raise RuntimeError("temporary qualification root escaped work-root")
        self.result["test_root"] = str(self.test_root)

    def _start_daemon(self) -> None:
        assert self.test_root is not None
        socket_path = self.test_root / "ublk.sock"
        cache_dir = self.test_root / "cache"
        resize_cache_dir = self.test_root / "resize-cache"
        cache_dir.mkdir()
        resize_cache_dir.mkdir()
        global_config = self.test_root / "global.json"
        self._write_json(
            global_config,
            {
                "registryFsVersion": "v2",
                "nrIoRings": 1,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": str(cache_dir),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                },
                "download": {"enable": False},
            },
        )
        resize_global_config = self.test_root / "resize-global.json"
        self._write_json(
            resize_global_config,
            {
                "registryFsVersion": "v2",
                "nrIoRings": 1,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": str(resize_cache_dir),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                },
                "download": {"enable": False},
            },
        )
        log_path = self.test_root / "ublk-daemon.log"
        log_handle = log_path.open("w", encoding="utf-8")
        self.daemon = subprocess.Popen(
            [
                str(self.daemon_binary),
                "--socket-path",
                str(socket_path),
                "--global-config",
                str(global_config),
                "--resize-global-config",
                str(resize_global_config),
                "--metrics-listen-addr",
                "",
                "--log-level",
                "info",
            ],
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        log_handle.close()
        self.client = AgentEnvUblkClient(socket_path)
        try:
            self.client.wait_ready(timeout_seconds=30)
        except BaseException:
            self.daemon.poll()
            tail = log_path.read_text(encoding="utf-8", errors="replace")[-4000:]
            raise RuntimeError(f"ublk daemon failed readiness:\n{tail}")

    def _create_initial_device(self) -> StorageNativeDevice:
        assert self.test_root is not None
        assert self.client is not None
        source = self.test_root / "source-empty.json"
        self._write_json(
            source,
            {"lowers": [], "upper": {}, "resultFile": ""},
        )
        runtime = self.test_root / "runtime-initial"
        device = self.client.create_runtime_device(
            source_image_config=source,
            global_config=self.test_root / "global.json",
            runtime_dir=runtime,
            virtual_size=self.virtual_size,
            owner_id="qualification-initial",
            upper_mode=self.upper_mode,
        )
        self._register_device(device)
        return device

    def _populate_and_verify_initial(self, volume: Path) -> None:
        assert self.test_root is not None
        lower = self.test_root / "overlay-lower"
        merged = self.test_root / "overlay-merged"
        lower.mkdir()
        merged.mkdir()
        (lower / "copy-up.txt").write_text("lower\n", encoding="utf-8")
        upper = volume / "overlay-upper"
        work = volume / "overlay-work"
        upper.mkdir()
        work.mkdir()
        self._mount(
            Path("overlay"),
            merged,
            "-t",
            "overlay",
            "-o",
            f"lowerdir={lower},upperdir={upper},workdir={work}",
        )
        with (merged / "copy-up.txt").open("a", encoding="utf-8") as handle:
            handle.write("upper\n")
            handle.flush()
            os.fsync(handle.fileno())
        if (lower / "copy-up.txt").read_text(encoding="utf-8") != "lower\n":
            raise RuntimeError("overlay copy-up mutated the lower directory")
        if (merged / "copy-up.txt").read_text(encoding="utf-8") != "lower\nupper\n":
            raise RuntimeError("overlay copy-up content mismatch")

        fs_dir = volume / "fs-semantics"
        fs_dir.mkdir()
        original = fs_dir / "original.txt"
        original.write_bytes(b"filesystem-state\n")
        os.setxattr(original, b"user.ucloud", b"storage-native")
        self._command("setfacl", "-m", "u:65534:r--", str(original))
        os.link(original, fs_dir / "hardlink.txt")
        renamed = fs_dir / "renamed.txt"
        original.rename(renamed)
        os.symlink("renamed.txt", fs_dir / "symlink.txt")
        self._verify_fs_semantics(fs_dir)

        active = volume / "active-memory"
        active.mkdir()
        memory_path = active / "application-memory.bin"
        with memory_path.open("w+b") as handle:
            handle.truncate(MEMORY_SIZE)
            with mmap.mmap(handle.fileno(), MEMORY_SIZE) as mapping:
                for offset, payload in SENTINELS.items():
                    mapping[offset : offset + len(payload)] = payload
                mapping[HOLE_OFFSET : HOLE_OFFSET + HOLE_SIZE] = b"\xa5" * HOLE_SIZE
                mapping.flush()
            os.fsync(handle.fileno())
        self._command(
            "fallocate",
            "--punch-hole",
            "--keep-size",
            "-o",
            str(HOLE_OFFSET),
            "-l",
            str(HOLE_SIZE),
            str(memory_path),
        )
        self._verify_memory(memory_path)
        hibernate = volume / "hibernate-1"
        hibernate.mkdir()
        memory_path.rename(hibernate / memory_path.name)
        self._sync_directory(active)
        self._sync_directory(hibernate)
        self._command("sync")

    def _freeze_and_seal(self, device: StorageNativeDevice, mount_path: Path):
        assert self.test_root is not None
        assert self.client is not None
        layers = self.test_root / "layers"
        layers.mkdir()
        output = layers / "generation-1.commit"
        self._command("fsfreeze", "--freeze", str(mount_path))
        try:
            layer = self.client.restack_snapshot(device.device_id, output)
        finally:
            self._command("fsfreeze", "--unfreeze", str(mount_path))
        if not output.is_file():
            raise RuntimeError("restack did not create the snapshot layer")
        if layer is not None and output.stat().st_size != layer.size:
            raise RuntimeError("snapshot descriptor size does not match the layer")
        self.result["layer"] = asdict(layer) if layer is not None else None
        return layer

    def _create_resumed_device(self, layer) -> StorageNativeDevice:
        assert self.test_root is not None
        assert self.client is not None
        layer_path = self.test_root / "layers" / "generation-1.commit"
        source = self.test_root / "source-resumed.json"
        lower = {"file": str(layer_path)}
        if layer is not None:
            lower.update({"digest": layer.digest, "size": layer.size})
        self._write_json(
            source,
            {
                "repoBlobUrl": "",
                "lowers": [lower],
                "upper": {},
                "resultFile": "",
            },
        )
        device = self.client.create_runtime_device(
            source_image_config=source,
            global_config=self.test_root / "global.json",
            runtime_dir=self.test_root / "runtime-resumed",
            virtual_size=self.virtual_size,
            owner_id="qualification-resumed",
            upper_mode=self.upper_mode,
        )
        self._register_device(device)
        return device

    def _verify_reconstructed(self, volume: Path) -> None:
        assert self.test_root is not None
        self._verify_fs_semantics(volume / "fs-semantics")
        memory = volume / "hibernate-1" / "application-memory.bin"
        self._verify_memory(memory)
        active = volume / "active-memory"
        memory.rename(active / memory.name)
        self._sync_directory(active)
        self._sync_directory(volume / "hibernate-1")

        merged = self.test_root / "overlay-resumed"
        merged.mkdir()
        self._mount(
            Path("overlay"),
            merged,
            "-t",
            "overlay",
            "-o",
            (
                f"lowerdir={self.test_root / 'overlay-lower'},"
                f"upperdir={volume / 'overlay-upper'},"
                f"workdir={volume / 'overlay-work'}"
            ),
        )
        if (merged / "copy-up.txt").read_text(encoding="utf-8") != "lower\nupper\n":
            raise RuntimeError("reconstructed overlay content mismatch")
        (merged / "post-resume.txt").write_text("new upper\n", encoding="utf-8")
        self._command("sync")
        self._unmount(merged)

    def _gvisor_create_and_park(self, volume: Path) -> None:
        assert self.test_root is not None
        assert self.runsc is not None
        assert self.conformance_workload is not None
        assert self.noop_workload is not None
        bundle = self.test_root / "gvisor-bundle"
        rootfs = bundle / "rootfs"
        rootfs.mkdir(parents=True)
        for directory in ("dev", "proc", "run", "sys", "tmp"):
            (rootfs / directory).mkdir()
        shutil.copyfile(
            self.conformance_workload,
            rootfs / "conformance-workload",
        )
        shutil.copyfile(self.noop_workload, rootfs / "noop")
        (rootfs / "conformance-workload").chmod(0o755)
        (rootfs / "noop").chmod(0o755)

        namespace = f"ucloud-storage-native-{os.getpid()}"
        self.result["gvisor_network_namespace"] = namespace
        self._command("ip", "netns", "add", namespace)
        self._command(
            "ip",
            "netns",
            "exec",
            namespace,
            "ip",
            "link",
            "set",
            "lo",
            "up",
        )
        memory_directory = "gvisor-qualifier.sandbox-1"
        memory_root = volume / "gvisor-memory"
        (memory_root / memory_directory).mkdir(mode=0o700, parents=True)
        checkpoint = volume / "gvisor-checkpoint" / "generation-1"
        checkpoint.mkdir(mode=0o700, parents=True)
        self._write_json(
            bundle / "config.json",
            self._gvisor_config(namespace, memory_directory),
        )

        common = self._gvisor_common(memory_root)
        state = self._gvisor_state()
        self._gvisor_command(
            *common,
            "create",
            f"--bundle={bundle}",
            "storage-native-qualifier",
            capture=False,
        )
        self._gvisor_command(
            *state,
            "start",
            "storage-native-qualifier",
            capture=False,
        )
        initial = self._gvisor_command(
            *state,
            "exec",
            "storage-native-qualifier",
            "/conformance-workload",
            "client",
        ).stdout.strip()
        if not initial.startswith("ok "):
            raise RuntimeError(f"initial gVisor conformance failed: {initial!r}")
        raw_state = json.loads(
            self._gvisor_command(
                *state,
                "state",
                "storage-native-qualifier",
            ).stdout
        )
        sentry_pid = int(raw_state["pid"])
        started = time.monotonic()
        self._gvisor_command(
            *common,
            "checkpoint",
            "--hibernate",
            f"--image-path={checkpoint}",
            "storage-native-qualifier",
            timeout=120,
        )
        park_seconds = time.monotonic() - started
        if not Path(f"/proc/{sentry_pid}").exists():
            raise RuntimeError("gVisor sentry died before explicit release")
        self._gvisor_command(
            *state,
            "delete",
            "--force",
            "storage-native-qualifier",
            capture=False,
        )
        if Path(f"/proc/{sentry_pid}").exists():
            raise RuntimeError("gVisor sentry survived explicit release")
        memory_artifact = checkpoint / "application_memory.img"
        if not memory_artifact.is_file():
            raise RuntimeError("gVisor hibernate did not capture application memory")
        artifacts = {
            path.name: {
                "allocated_bytes": path.stat().st_blocks * 512,
                "file_bytes": path.stat().st_size,
            }
            for path in sorted(checkpoint.iterdir())
            if path.is_file()
        }
        self.result["gvisor"] = {
            "initial_response": initial,
            "park_seconds": park_seconds,
            "checkpoint_artifacts": artifacts,
        }

    def _gvisor_restore_and_verify(
        self,
        device: StorageNativeDevice,
        volume: Path,
    ) -> None:
        assert self.test_root is not None
        memory_root = volume / "gvisor-memory"
        checkpoint = volume / "gvisor-checkpoint" / "generation-1"
        common = self._gvisor_common(memory_root)
        state = self._gvisor_state()
        self._command("sync")
        Path("/proc/sys/vm/drop_caches").write_text("3\n", encoding="ascii")

        before = self._device_sectors(device)
        started = time.monotonic()
        self._gvisor_command(
            *common,
            "restore",
            "--detach",
            "--background",
            f"--image-path={checkpoint}",
            f"--bundle={self.test_root / 'gvisor-bundle'}",
            "storage-native-qualifier",
            timeout=120,
            capture=False,
        )
        restore_seconds = time.monotonic() - started
        after_restore = self._device_sectors(device)

        started = time.monotonic()
        self._gvisor_command(
            *state,
            "exec",
            "storage-native-qualifier",
            "/noop",
        )
        noop_seconds = time.monotonic() - started
        after_noop = self._device_sectors(device)

        started = time.monotonic()
        response = self._gvisor_command(
            *state,
            "exec",
            "storage-native-qualifier",
            "/conformance-workload",
            "client",
        ).stdout.strip()
        conformance_seconds = time.monotonic() - started
        after_conformance = self._device_sectors(device)
        if not response.startswith("ok "):
            raise RuntimeError(
                f"reconstructed gVisor conformance failed: {response!r}"
            )
        self._gvisor_command(
            *state,
            "delete",
            "--force",
            "storage-native-qualifier",
            capture=False,
        )

        gvisor = self.result["gvisor"]
        memory_allocated = int(
            gvisor["checkpoint_artifacts"]["application_memory.img"][
                "allocated_bytes"
            ]
        )
        restore_read_bytes = (after_restore[0] - before[0]) * 512
        noop_read_bytes = (after_noop[0] - after_restore[0]) * 512
        conformance_read_bytes = (
            after_conformance[0] - after_noop[0]
        ) * 512
        gvisor["restore"] = {
            "conformance_read_bytes": conformance_read_bytes,
            "conformance_seconds": conformance_seconds,
            "noop_read_bytes": noop_read_bytes,
            "noop_seconds": noop_seconds,
            "response": response,
            "restore_read_bytes": restore_read_bytes,
            "restore_seconds": restore_seconds,
        }
        foreground_read_bytes = restore_read_bytes + noop_read_bytes
        if (
            memory_allocated >= 8 * MIB
            and foreground_read_bytes >= memory_allocated * 4 // 5
        ):
            raise RuntimeError(
                "gVisor restore and noop read at least 80% of the "
                "application-memory allocation"
            )

    def _gvisor_config(
        self,
        namespace: str,
        memory_directory: str,
    ) -> dict[str, Any]:
        return {
            "annotations": {
                "dev.gvisor.internal.application-memory-directory": (
                    memory_directory
                )
            },
            "hostname": "storage-native-qualifier",
            "linux": {
                "namespaces": [
                    {"type": "pid"},
                    {"path": f"/run/netns/{namespace}", "type": "network"},
                    {"type": "ipc"},
                    {"type": "uts"},
                    {"type": "mount"},
                ],
                "resources": {
                    "memory": {
                        "limit": 512 * MIB,
                        "swap": 512 * MIB,
                    }
                },
            },
            "mounts": [
                {
                    "destination": "/proc",
                    "options": ["nosuid", "noexec", "nodev"],
                    "source": "proc",
                    "type": "proc",
                },
                {
                    "destination": "/dev",
                    "options": ["nosuid", "mode=755", "size=65536k"],
                    "source": "tmpfs",
                    "type": "tmpfs",
                },
                {
                    "destination": "/dev/pts",
                    "options": [
                        "nosuid",
                        "noexec",
                        "newinstance",
                        "ptmxmode=0666",
                        "mode=0620",
                        "gid=5",
                    ],
                    "source": "devpts",
                    "type": "devpts",
                },
                {
                    "destination": "/dev/shm",
                    "options": [
                        "nosuid",
                        "noexec",
                        "nodev",
                        "mode=1777",
                        "size=65536k",
                    ],
                    "source": "shm",
                    "type": "tmpfs",
                },
                {
                    "destination": "/dev/mqueue",
                    "options": ["nosuid", "noexec", "nodev"],
                    "source": "mqueue",
                    "type": "mqueue",
                },
                {
                    "destination": "/sys",
                    "options": ["nosuid", "noexec", "nodev", "ro"],
                    "source": "sysfs",
                    "type": "sysfs",
                },
                {
                    "destination": "/tmp",
                    "options": [
                        "nosuid",
                        "nodev",
                        "mode=1777",
                        "size=67108864",
                    ],
                    "source": "tmpfs",
                    "type": "tmpfs",
                },
            ],
            "ociVersion": "1.0.2",
            "process": {
                "args": ["/conformance-workload", "server"],
                "capabilities": {
                    name: []
                    for name in (
                        "bounding",
                        "effective",
                        "inheritable",
                        "permitted",
                    )
                },
                "cwd": "/",
                "env": ["PATH=/"],
                "noNewPrivileges": True,
                "terminal": False,
                "user": {"gid": 0, "uid": 0},
            },
            "root": {"path": "rootfs", "readonly": False},
        }

    def _gvisor_common(self, memory_root: Path) -> list[str]:
        assert self.runsc is not None
        assert self.test_root is not None
        return [
            str(self.runsc),
            f"--root={self.test_root / 'gvisor-runsc'}",
            f"--application-memory-file-dir={memory_root}",
            "--network=sandbox",
        ]

    def _gvisor_state(self) -> list[str]:
        assert self.runsc is not None
        assert self.test_root is not None
        return [
            str(self.runsc),
            f"--root={self.test_root / 'gvisor-runsc'}",
        ]

    @staticmethod
    def _device_sectors(device: StorageNativeDevice) -> tuple[int, int]:
        stat_path = Path("/sys/class/block") / device.device_path.name / "stat"
        fields = [int(field) for field in stat_path.read_text().split()]
        return fields[2], fields[6]

    @staticmethod
    def _gvisor_command(
        *args: str,
        timeout: float = 60,
        capture: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            args,
            check=True,
            text=True,
            stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
            stderr=subprocess.PIPE if capture else subprocess.DEVNULL,
            timeout=timeout,
        )

    def _verify_fs_semantics(self, fs_dir: Path) -> None:
        renamed = fs_dir / "renamed.txt"
        hardlink = fs_dir / "hardlink.txt"
        symlink = fs_dir / "symlink.txt"
        if renamed.read_bytes() != b"filesystem-state\n":
            raise RuntimeError("renamed file content mismatch")
        if os.getxattr(renamed, b"user.ucloud") != b"storage-native":
            raise RuntimeError("xattr did not survive")
        if renamed.stat().st_ino != hardlink.stat().st_ino:
            raise RuntimeError("hard link identity did not survive")
        if not stat.S_ISLNK(symlink.lstat().st_mode):
            raise RuntimeError("symbolic link did not survive")
        if symlink.resolve() != renamed:
            raise RuntimeError("symbolic link target did not follow rename")
        acl = self._command("getfacl", "--absolute-names", str(renamed)).stdout
        if "user:nobody:r--" not in acl and "user:65534:r--" not in acl:
            raise RuntimeError("ACL did not survive")

    @staticmethod
    def _verify_memory(memory_path: Path) -> None:
        if memory_path.stat().st_size != MEMORY_SIZE:
            raise RuntimeError("application memory size mismatch")
        with memory_path.open("r+b") as handle:
            with mmap.mmap(handle.fileno(), MEMORY_SIZE) as mapping:
                for offset, payload in SENTINELS.items():
                    if mapping[offset : offset + len(payload)] != payload:
                        raise RuntimeError(f"memory sentinel mismatch at {offset}")
                if mapping[HOLE_OFFSET : HOLE_OFFSET + HOLE_SIZE] != b"\0" * HOLE_SIZE:
                    raise RuntimeError("punched memory extent did not read as zero")

    def _verify_enospc(self, volume: Path) -> None:
        target = volume / "enospc-reservation"
        with target.open("w+b") as handle:
            try:
                os.posix_fallocate(handle.fileno(), 0, self.virtual_size)
            except OSError as exc:
                if exc.errno != errno.ENOSPC:
                    raise
                self.result["enospc_errno"] = exc.errno
            else:
                raise RuntimeError(
                    "filesystem allocated the complete logical device without ENOSPC"
                )

    def _register_device(self, device: StorageNativeDevice) -> None:
        if device.virtual_size != self.virtual_size:
            raise RuntimeError("ublk daemon changed the requested device size")
        self.devices[device.device_id] = device

    def _delete_device(self, device: StorageNativeDevice) -> None:
        assert self.client is not None
        self.client.delete(device.device_id)
        self.devices.pop(device.device_id, None)

    def _mount(self, source: Path, target: Path, *options: str) -> None:
        self._command("mount", *options, str(source), str(target))
        self.mounts.append(target)

    def _unmount(self, target: Path) -> None:
        self._command("umount", str(target))
        with contextlib.suppress(ValueError):
            self.mounts.remove(target)

    @contextlib.contextmanager
    def _phase(
        self,
        name: str,
        device: StorageNativeDevice | None = None,
    ) -> Iterator[None]:
        before = self._counters(device)
        started = time.monotonic()
        try:
            yield
        finally:
            elapsed = time.monotonic() - started
            after = self._counters(device)
            self.phases[name] = {
                "seconds": elapsed,
                "counters": after.minus(before),
            }

    def _counters(self, device: StorageNativeDevice | None) -> Counters:
        process_io = {"read_bytes": 0, "write_bytes": 0}
        user_ticks = system_ticks = 0
        if self.daemon is not None and self.daemon.poll() is None:
            pid = self.daemon.pid
            with contextlib.suppress(FileNotFoundError, ProcessLookupError):
                for line in Path(f"/proc/{pid}/io").read_text().splitlines():
                    key, raw_value = line.split(":", 1)
                    if key in process_io:
                        process_io[key] = int(raw_value)
                fields = Path(f"/proc/{pid}/stat").read_text().split()
                user_ticks = int(fields[13])
                system_ticks = int(fields[14])
        read_sectors = write_sectors = 0
        if device is not None:
            stat_path = Path("/sys/class/block") / device.device_path.name / "stat"
            with contextlib.suppress(FileNotFoundError):
                fields = [int(field) for field in stat_path.read_text().split()]
                read_sectors = fields[2]
                write_sectors = fields[6]
        return Counters(
            daemon_read_bytes=process_io["read_bytes"],
            daemon_write_bytes=process_io["write_bytes"],
            daemon_user_ticks=user_ticks,
            daemon_system_ticks=system_ticks,
            device_read_sectors=read_sectors,
            device_write_sectors=write_sectors,
        )

    def _cleanup(self) -> None:
        if self.runsc is not None and self.test_root is not None:
            subprocess.run(
                [
                    str(self.runsc),
                    f"--root={self.test_root / 'gvisor-runsc'}",
                    "delete",
                    "--force",
                    "storage-native-qualifier",
                ],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        for mount_path in reversed(self.mounts.copy()):
            subprocess.run(
                ["umount", "-l", str(mount_path)],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        self.mounts.clear()
        if self.client is not None:
            for device_id in list(self.devices):
                with contextlib.suppress(Exception):
                    self.client.delete(device_id)
                self.devices.pop(device_id, None)
            with contextlib.suppress(Exception):
                self.client.shutdown()
        if self.daemon is not None:
            try:
                self.daemon.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.daemon.terminate()
                try:
                    self.daemon.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.daemon.kill()
                    self.daemon.wait(timeout=10)
        namespace = self.result.get("gvisor_network_namespace")
        if isinstance(namespace, str) and namespace:
            subprocess.run(
                ["ip", "netns", "del", namespace],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    @staticmethod
    def _write_json(path: Path, payload: Any) -> None:
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def _sync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _command(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            args,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Destructively qualify a layered ublk sandbox volume"
    )
    parser.add_argument("--daemon", required=True, type=Path)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--size-gib", default=1, type=int)
    parser.add_argument(
        "--upper-mode",
        choices=("sparse", "logStructured", "hybridLogStructured"),
        default="hybridLogStructured",
    )
    parser.add_argument("--runsc", type=Path)
    parser.add_argument("--conformance-workload", type=Path)
    parser.add_argument("--noop-workload", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    qualifier = Qualifier(
        daemon_binary=args.daemon.resolve(),
        work_root=args.work_root.resolve(),
        output=args.output.resolve(),
        virtual_size=args.size_gib * GIB,
        upper_mode=args.upper_mode,
        runsc=args.runsc.resolve() if args.runsc else None,
        conformance_workload=(
            args.conformance_workload.resolve()
            if args.conformance_workload
            else None
        ),
        noop_workload=(
            args.noop_workload.resolve() if args.noop_workload else None
        ),
    )
    try:
        result = qualifier.run()
    except BaseException:
        print(json.dumps(qualifier.result, indent=2, sort_keys=True), file=sys.stderr)
        raise
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
