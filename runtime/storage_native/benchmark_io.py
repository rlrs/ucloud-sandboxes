#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
from dataclasses import asdict
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT))

from ucloud_sandboxes.storage_native import (  # noqa: E402
    AgentEnvUblkClient,
    StorageNativeDevice,
)


GIB = 1024 * 1024 * 1024


class IoBenchmark:
    def __init__(
        self,
        *,
        daemon_binary: Path,
        work_root: Path,
        output: Path,
        rounds: int,
        runtime_seconds: int,
        virtual_size: int,
        upper_mode: str,
        io_rings: int,
    ) -> None:
        self.daemon_binary = daemon_binary
        self.work_root = work_root
        self.output = output
        self.rounds = rounds
        self.runtime_seconds = runtime_seconds
        self.virtual_size = virtual_size
        self.upper_mode = upper_mode
        self.io_rings = io_rings
        self.root: Path | None = None
        self.daemon: subprocess.Popen[str] | None = None
        self.client: AgentEnvUblkClient | None = None
        self.device: StorageNativeDevice | None = None
        self.loop_device: Path | None = None
        self.mounts: list[Path] = []

    def run(self) -> dict[str, Any]:
        self._preflight()
        assert self.root is not None
        samples: list[dict[str, Any]] = []
        try:
            self._start_daemon()
            ublk_mount = self._create_ublk_xfs()
            native_mount = self._create_native_xfs()
            targets = {
                "native_xfs_loop": native_mount,
                "overlaybd_ublk_xfs": ublk_mount,
            }
            for round_number in range(self.rounds):
                order = list(targets)
                if round_number % 2:
                    order.reverse()
                for target_name in order:
                    mount_path = targets[target_name]
                    for workload in ("sequential_write", "random_mixed"):
                        samples.append(
                            self._fio_sample(
                                target_name,
                                mount_path,
                                round_number,
                                workload,
                            )
                        )
                    samples.append(
                        self._metadata_sample(
                            target_name,
                            mount_path,
                            round_number,
                        )
                    )
            seal = self._seal_sample(ublk_mount)
            summary, gates = self._summarize(samples)
            gates["seal_did_not_materialize_virtual_device"] = (
                seal["allocated_bytes"] < self.virtual_size
            )
            result = {
                "schema": 2,
                "status": "passed" if all(gates.values()) else "failed",
                "host": {
                    "uname": " ".join(os.uname()),
                    "virtual_size": self.virtual_size,
                    "upper_mode": self.upper_mode,
                    "io_rings": self.io_rings,
                },
                "rounds": self.rounds,
                "runtime_seconds": self.runtime_seconds,
                "samples": samples,
                "seal": seal,
                "summary": summary,
                "gates": gates,
            }
            self.output.parent.mkdir(parents=True, exist_ok=True)
            self.output.write_text(
                json.dumps(result, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return result
        finally:
            self._cleanup()

    def _preflight(self) -> None:
        if sys.platform != "linux" or os.geteuid() != 0:
            raise RuntimeError("storage-native I/O benchmark requires Linux root")
        if not self.daemon_binary.is_absolute() or not os.access(
            self.daemon_binary, os.X_OK
        ):
            raise ValueError("--daemon must be an absolute executable file")
        if not self.work_root.is_absolute() or not self.work_root.is_dir():
            raise ValueError("--work-root must be an existing absolute directory")
        if self.work_root == Path("/"):
            raise ValueError("--work-root may not be the filesystem root")
        if not self.output.is_absolute():
            raise ValueError("--output must be absolute")
        if self.rounds < 2 or self.runtime_seconds < 2:
            raise ValueError("rounds and runtime must both be at least two")
        if self.upper_mode not in {
            "sparse",
            "logStructured",
            "hybridLogStructured",
        }:
            raise ValueError("unsupported overlaybd upper mode")
        if self.io_rings < 1:
            raise ValueError("I/O ring count must be positive")
        if self.virtual_size < 4 * GIB or self.virtual_size % GIB:
            raise ValueError("the benchmark device must be whole-GiB and at least 4 GiB")
        for tool in (
            "fio",
            "fsfreeze",
            "losetup",
            "mkfs.xfs",
            "mount",
            "sync",
            "umount",
        ):
            if shutil.which(tool) is None:
                raise RuntimeError(f"required host tool is missing: {tool}")
        if not Path("/dev/ublk-control").exists():
            raise RuntimeError("/dev/ublk-control is unavailable")
        raw_root = tempfile.mkdtemp(
            prefix="ucloud-storage-native-io-",
            dir=self.work_root,
        )
        self.root = Path(raw_root).resolve()
        if self.root.parent != self.work_root.resolve():
            raise RuntimeError("benchmark root escaped work-root")

    def _start_daemon(self) -> None:
        assert self.root is not None
        cache = self.root / "cache"
        cache.mkdir()
        global_config = self.root / "global.json"
        self._write_json(
            global_config,
            {
                "registryFsVersion": "v2",
                "nrIoRings": self.io_rings,
                "cacheConfig": {
                    "cacheType": "file",
                    "cacheDir": str(cache),
                    "cacheSizeGB": 1,
                    "refillSize": 262144,
                },
                "download": {"enable": False},
            },
        )
        socket_path = self.root / "ublk.sock"
        log_handle = (self.root / "ublk-daemon.log").open("w", encoding="utf-8")
        self.daemon = subprocess.Popen(
            [
                str(self.daemon_binary),
                "--socket-path",
                str(socket_path),
                "--global-config",
                str(global_config),
                "--metrics-listen-addr",
                "",
            ],
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        log_handle.close()
        self.client = AgentEnvUblkClient(socket_path)
        self.client.wait_ready()

    def _create_ublk_xfs(self) -> Path:
        assert self.root is not None
        assert self.client is not None
        source = self.root / "source-empty.json"
        self._write_json(source, {"lowers": [], "upper": {}, "resultFile": ""})
        self.device = self.client.create_runtime_device(
            source_image_config=source,
            global_config=self.root / "global.json",
            runtime_dir=self.root / "runtime",
            virtual_size=self.virtual_size,
            owner_id="io-benchmark",
            upper_mode=self.upper_mode,
        )
        self._format_xfs(self.device.device_path)
        mount_path = self.root / "ublk-xfs"
        mount_path.mkdir()
        self._mount(self.device.device_path, mount_path)
        return mount_path

    def _create_native_xfs(self) -> Path:
        assert self.root is not None
        backing = self.root / "native-loop.img"
        descriptor = os.open(
            backing,
            os.O_CREAT | os.O_EXCL | os.O_RDWR,
            0o600,
        )
        try:
            os.posix_fallocate(descriptor, 0, self.virtual_size)
        finally:
            os.close(descriptor)
        raw_loop = self._command(
            "losetup",
            "--find",
            "--show",
            str(backing),
        ).stdout.strip()
        self.loop_device = Path(raw_loop)
        self._format_xfs(self.loop_device)
        mount_path = self.root / "native-xfs"
        mount_path.mkdir()
        self._mount(self.loop_device, mount_path)
        return mount_path

    def _fio_sample(
        self,
        target_name: str,
        mount_path: Path,
        round_number: int,
        workload: str,
    ) -> dict[str, Any]:
        target = mount_path / "fio.data"
        target.unlink(missing_ok=True)
        self._drop_caches()
        common = [
            "fio",
            f"--name={workload}",
            f"--filename={target}",
            "--output-format=json",
            "--ioengine=libaio",
            "--direct=1",
            "--group_reporting=1",
        ]
        if workload == "sequential_write":
            command = [
                *common,
                "--rw=write",
                "--bs=1M",
                "--size=1G",
                "--iodepth=16",
                "--end_fsync=1",
            ]
        else:
            command = [
                *common,
                "--rw=randrw",
                "--rwmixread=70",
                "--bs=4K",
                "--size=1G",
                f"--runtime={self.runtime_seconds}",
                "--time_based=1",
                "--iodepth=32",
            ]
        started = time.monotonic()
        payload = json.loads(
            self._command(*command, timeout=self.runtime_seconds + 120).stdout
        )
        seconds = time.monotonic() - started
        target.unlink(missing_ok=True)
        self._command("sync", "-f", str(mount_path))
        job = payload["jobs"][0]
        return {
            "kind": "fio",
            "round": round_number,
            "seconds": seconds,
            "target": target_name,
            "workload": workload,
            "read_bw_bytes": float(job["read"]["bw_bytes"]),
            "read_iops": float(job["read"]["iops"]),
            "write_bw_bytes": float(job["write"]["bw_bytes"]),
            "write_iops": float(job["write"]["iops"]),
        }

    def _metadata_sample(
        self,
        target_name: str,
        mount_path: Path,
        round_number: int,
    ) -> dict[str, Any]:
        root = mount_path / "metadata"
        root.mkdir()
        self._drop_caches()
        count = 4000
        started = time.monotonic()
        for index in range(count):
            path = root / f"file-{index:05d}"
            path.write_bytes(index.to_bytes(8, "little"))
        self._sync_directory(root)
        create_seconds = time.monotonic() - started
        started = time.monotonic()
        checksum = 0
        for index in range(count):
            checksum += (root / f"file-{index:05d}").stat().st_size
        stat_seconds = time.monotonic() - started
        started = time.monotonic()
        for index in range(count):
            source = root / f"file-{index:05d}"
            target = root / f"renamed-{index:05d}"
            source.rename(target)
        for index in range(count):
            (root / f"renamed-{index:05d}").unlink()
        self._sync_directory(root)
        rename_delete_seconds = time.monotonic() - started
        root.rmdir()
        return {
            "checksum": checksum,
            "count": count,
            "create_seconds": create_seconds,
            "kind": "metadata",
            "rename_delete_seconds": rename_delete_seconds,
            "round": round_number,
            "stat_seconds": stat_seconds,
            "target": target_name,
            "workload": "metadata",
        }

    def _seal_sample(self, mount_path: Path) -> dict[str, Any]:
        assert self.root is not None
        assert self.client is not None
        assert self.device is not None
        output = self.root / "sealed.commit"
        self._command("sync", "-f", str(mount_path))
        self._command("fsfreeze", "--freeze", str(mount_path))
        started = time.monotonic()
        try:
            descriptor = self.client.restack_snapshot(
                self.device.device_id,
                output,
            )
        finally:
            self._command("fsfreeze", "--unfreeze", str(mount_path))
        seconds = time.monotonic() - started
        metadata = output.stat()
        return {
            "allocated_bytes": metadata.st_blocks * 512,
            "descriptor": asdict(descriptor) if descriptor is not None else None,
            "logical_bytes": metadata.st_size,
            "seconds": seconds,
        }

    @staticmethod
    def _summarize(
        samples: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, bool]]:
        summary: dict[str, Any] = {}
        for target in ("native_xfs_loop", "overlaybd_ublk_xfs"):
            seq = [
                sample["write_bw_bytes"]
                for sample in samples
                if sample["target"] == target
                and sample["workload"] == "sequential_write"
            ]
            rand = [
                sample["read_iops"] + sample["write_iops"]
                for sample in samples
                if sample["target"] == target
                and sample["workload"] == "random_mixed"
            ]
            metadata = [
                sample["create_seconds"]
                + sample["stat_seconds"]
                + sample["rename_delete_seconds"]
                for sample in samples
                if sample["target"] == target
                and sample["workload"] == "metadata"
            ]
            summary[target] = {
                "metadata_seconds_median": statistics.median(metadata),
                "random_mixed_iops_median": statistics.median(rand),
                "sequential_write_bw_bytes_median": statistics.median(seq),
            }
        native = summary["native_xfs_loop"]
        ublk = summary["overlaybd_ublk_xfs"]
        ratios = {
            "metadata_time_ratio": (
                ublk["metadata_seconds_median"]
                / native["metadata_seconds_median"]
            ),
            "random_mixed_iops_ratio": (
                ublk["random_mixed_iops_median"]
                / native["random_mixed_iops_median"]
            ),
            "sequential_write_bw_ratio": (
                ublk["sequential_write_bw_bytes_median"]
                / native["sequential_write_bw_bytes_median"]
            ),
        }
        summary["ratios"] = ratios
        gates = {
            "metadata_within_15_percent": ratios["metadata_time_ratio"] <= 1.15,
            "random_mixed_within_15_percent": (
                ratios["random_mixed_iops_ratio"] >= 0.85
            ),
            "sequential_write_within_15_percent": (
                ratios["sequential_write_bw_ratio"] >= 0.85
            ),
        }
        return summary, gates

    def _format_xfs(self, device: Path) -> None:
        self._command(
            "mkfs.xfs",
            "-f",
            "-m",
            "reflink=1",
            "-n",
            "ftype=1",
            str(device),
        )

    def _mount(self, device: Path, target: Path) -> None:
        self._command("mount", "-o", "noatime", str(device), str(target))
        self.mounts.append(target)

    @staticmethod
    def _drop_caches() -> None:
        subprocess.run(["sync"], check=True)
        Path("/proc/sys/vm/drop_caches").write_text("3\n", encoding="ascii")

    @staticmethod
    def _sync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _cleanup(self) -> None:
        for mount_path in reversed(self.mounts):
            subprocess.run(
                ["umount", "-l", str(mount_path)],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        self.mounts.clear()
        if self.loop_device is not None:
            subprocess.run(
                ["losetup", "--detach", str(self.loop_device)],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        if self.client is not None:
            if self.device is not None:
                with contextlib.suppress(Exception):
                    self.client.delete(self.device.device_id)
            with contextlib.suppress(Exception):
                self.client.shutdown()
        if self.daemon is not None:
            try:
                self.daemon.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.daemon.terminate()
                self.daemon.wait(timeout=10)

    @staticmethod
    def _write_json(path: Path, payload: Any) -> None:
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def _command(
        *args: str,
        timeout: float = 300,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            args,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare OverlayBD ublk XFS against native loopback XFS"
    )
    parser.add_argument("--daemon", required=True, type=Path)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rounds", default=3, type=int)
    parser.add_argument("--runtime-seconds", default=8, type=int)
    parser.add_argument("--size-gib", default=4, type=int)
    parser.add_argument(
        "--upper-mode",
        choices=("sparse", "logStructured", "hybridLogStructured"),
        default="hybridLogStructured",
    )
    parser.add_argument("--io-rings", default=1, type=int)
    args = parser.parse_args()
    benchmark = IoBenchmark(
        daemon_binary=args.daemon.resolve(),
        work_root=args.work_root.resolve(),
        output=args.output.resolve(),
        rounds=args.rounds,
        runtime_seconds=args.runtime_seconds,
        virtual_size=args.size_gib * GIB,
        upper_mode=args.upper_mode,
        io_rings=args.io_rings,
    )
    result = benchmark.run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
