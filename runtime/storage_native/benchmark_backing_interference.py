#!/usr/bin/env python3
"""Compare real guest memory/SQLite interference on coupled and split backing.

Uses the native qualifier's owned daemon/device/cleanup lifecycle. Run as root
on an isolated VM, on an ordinary XFS project-quota filesystem, never production.
The fixture dirties guest anonymous memory while committing SQLite transactions;
a periodic workspace sync models filesystem maintenance. This is an interference
experiment, not a checkpoint correctness or density qualification.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import ctypes
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time

from qualify_volume import GIB, MIB, Qualifier
from ucloud_sandboxes.resource_evidence import disk_rates, read_leaf_disks


def sync_filesystem(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        library = ctypes.CDLL(None, use_errno=True)
        if library.syncfs(descriptor):
            raise OSError(ctypes.get_errno(), "syncfs failed")
    finally:
        os.close(descriptor)


class InterferenceBenchmark(Qualifier):
    def __init__(self, *, layout, seconds, resident_mb, dirty_mb, sync_interval, entropy_percent=0, **kwargs):
        super().__init__(**kwargs)
        self.layout = layout
        self.seconds = seconds
        self.resident_mb = resident_mb
        self.dirty_mb = dirty_mb
        self.entropy_percent = entropy_percent
        self.sync_interval = sync_interval

    def run(self):
        self._preflight()
        assert self.test_root is not None and self.runsc is not None
        assert self.conformance_workload is not None
        result = {
            "layout": self.layout, "passed": False, "seconds": self.seconds,
            "resident_mb": self.resident_mb, "dirty_mb": self.dirty_mb,
            "entropy_percent": self.entropy_percent,
            "workspace_sync_interval": self.sync_interval,
            "runtime_sha256": hashlib.sha256(self.runsc.read_bytes()).hexdigest(),
            "workload_sha256": hashlib.sha256(self.conformance_workload.read_bytes()).hexdigest(),
            "scope": "isolated whole-VM physical I/O; guest SQLite and dirty memory; no checkpoint",
        }
        stop = threading.Event()
        sync_thread = None
        errors = []
        sync_times = []
        memory_samples = []
        try:
            self._start_daemon()
            device = self._create_initial_device()
            self._command("mkfs.xfs", "-f", str(device.device_path))
            volume = self.test_root / "workspace-volume"
            volume.mkdir()
            self._mount(device.device_path, volume, "-o", self._mount_options)
            bundle = self.test_root / "gvisor-bundle"
            rootfs = bundle / "rootfs"
            rootfs.mkdir(parents=True)
            for name in ("proc", "dev", "sys", "tmp", "workspace"):
                (rootfs / name).mkdir()
            shutil.copyfile(self.conformance_workload, rootfs / "interference")
            (rootfs / "interference").chmod(0o755)
            workspace = volume / "workspace"
            workspace.mkdir()
            memory_root = (volume if self.layout == "coupled" else self.test_root) / "memory"
            if self.layout == "ram":
                memory_root.mkdir(mode=0o700)
                self._mount(Path("tmpfs"), memory_root, "-t", "tmpfs", "-o",
                            f"size={(self.resident_mb + 512) * MIB},noswap,mode=0700")
            memory_dir = "interference.sandbox-1"
            (memory_root / memory_dir).mkdir(parents=True, mode=0o700)
            config = self._gvisor_config("", memory_dir)
            config["linux"]["cgroupsPath"] = "/ucloud-interference-" + self.test_root.name
            config["linux"]["namespaces"] = [
                item for item in config["linux"]["namespaces"] if item["type"] != "network"
            ]
            config["linux"]["resources"]["memory"]["limit"] = (self.resident_mb + 512) * MIB
            config["linux"]["resources"]["memory"]["swap"] = (self.resident_mb + 512) * MIB
            config["process"]["args"] = ["/interference", str(self.seconds), str(self.resident_mb), str(self.dirty_mb), str(self.entropy_percent)]
            config["mounts"].append({"type": "bind", "source": str(workspace),
                "destination": "/workspace", "options": ["bind", "rw"]})
            self._write_json(bundle / "config.json", config)

            def maintenance():
                while not stop.wait(self.sync_interval):
                    started = time.monotonic()
                    cgroup = Path("/sys/fs/cgroup") / config["linux"]["cgroupsPath"].lstrip("/")
                    sample = {}
                    for key in ("memory.current", "memory.peak", "memory.max", "memory.events"):
                        try:
                            sample[key] = (cgroup / key).read_text().strip()
                        except OSError:
                            pass
                    memory_samples.append(sample)
                    try:
                        sync_filesystem(volume)
                    except Exception as exc:
                        errors.append(str(exc))
                        return
                    sync_times.append(time.monotonic() - started)

            sync_filesystem(volume)
            sync_filesystem(memory_root)
            sync_filesystem(self.work_root)
            before = read_leaf_disks(Path("/proc"), Path("/sys"))
            native_before = self._counters(device)
            began = time.monotonic()
            sync_thread = threading.Thread(target=maintenance, daemon=True)
            sync_thread.start()
            command = [
                str(self.runsc), f"--root={self.test_root / 'gvisor-runsc'}",
                f"--application-memory-file-dir={memory_root}", "--network=none",
            ]
            if self.layout == "ram":
                command.append("--application-memory-ram-backing=true")
            command.extend(["run", f"--bundle={bundle}", "storage-native-qualifier"])
            completed = subprocess.run(command, text=True, capture_output=True, timeout=self.seconds + 120)
            stop.set()
            sync_thread.join(timeout=120)
            if sync_thread.is_alive():
                raise RuntimeError("workspace sync failed to settle")
            if completed.returncode:
                raise RuntimeError(f"guest exited {completed.returncode}: " + completed.stderr[-4000:] + completed.stdout[-4000:])
            guest = json.loads(completed.stdout)
            if not guest["verified"] or not guest["commits"]:
                raise RuntimeError("guest failed integrity verification")
            if errors:
                raise RuntimeError("; ".join(errors))
            # Include outstanding writeback instead of hiding deferred writes.
            sync_filesystem(volume)
            sync_filesystem(memory_root)
            sync_filesystem(self.work_root)
            elapsed = time.monotonic() - began
            after = read_leaf_disks(Path("/proc"), Path("/sys"))
            result.update(guest=guest, elapsed_seconds=elapsed,
                workspace_sync_seconds=sync_times,
                cgroup_memory_samples=memory_samples,
                native_counters=self._counters(device).minus(native_before),
                physical_devices=[asdict(disk_rates(identity, name, counters,
                    before.get(identity, (None, None))[1], elapsed))
                    for identity, (name, counters) in after.items()] if before is not None and after is not None else None,
                passed=True)
            self._gvisor_command(*self._gvisor_state(), "delete", "--force", "storage-native-qualifier")
            if self.layout == "ram":
                self._unmount(memory_root)
            self._unmount(volume)
            self._delete_device(device)
        except Exception as exc:
            result["error"] = str(exc)
            raise
        finally:
            stop.set()
            if sync_thread is not None:
                sync_thread.join(timeout=120)
            self._cleanup()
            # runsc --network=none caches its namespace under its private root.
            # This run owns that root, including the otherwise persistent mount.
            null_namespace = self.test_root / "gvisor-runsc" / "null-netns"
            if subprocess.run(["mountpoint", "-q", str(null_namespace)]).returncode == 0:
                subprocess.run(["umount", str(null_namespace)], check=True)
            # Cleanup must not erase a still-mounted filesystem after an error.
            mounts = Path("/proc/self/mountinfo").read_text()
            if str(self.test_root) not in mounts:
                shutil.rmtree(self.test_root)
            else:
                result["passed"] = False
                result["retained_root"] = str(self.test_root)
            self.output.write_text(json.dumps(result, indent=2) + "\n")
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("daemon", "runsc", "workload", "work-root", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--layout", choices=("coupled", "split", "ram"), required=True)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--resident-mb", type=int, default=512)
    parser.add_argument("--dirty-mb", type=int, default=128)
    parser.add_argument("--entropy-percent", type=int, choices=range(101), default=0)
    parser.add_argument("--sync-interval", type=float, default=2)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 600 or not 0 < args.dirty_mb <= args.resident_mb <= 8192 or args.sync_interval <= 0:
        parser.error("invalid workload size or interval")
    result = InterferenceBenchmark(daemon_binary=args.daemon, runsc=args.runsc,
        work_root=args.work_root, output=args.output, virtual_size=4 * GIB,
        upper_mode="sparse", filesystem="xfs", conformance_workload=args.workload,
        noop_workload=args.workload, layout=args.layout, seconds=args.seconds,
        resident_mb=args.resident_mb, dirty_mb=args.dirty_mb,
        entropy_percent=args.entropy_percent,
        sync_interval=args.sync_interval).run()
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
