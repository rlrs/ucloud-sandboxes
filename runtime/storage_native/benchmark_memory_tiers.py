#!/usr/bin/env python3
"""Isolated native application-memory tier comparison, with unchanged runsc.

The fixture uses a host XFS root solely to isolate memory costs from ublk. It
does not qualify the product's storage/ownership handoff. Each sample verifies
the entire incompressible heap, an established TCP socket and an open SQLite WAL
writer. No global cache eviction or production inventory is used.
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

from qualify_volume import Qualifier

MIB = 1024**2


def command(*args, timeout=120):
    detached = any(str(arg) in {"create", "start", "restore"} for arg in args)
    result = subprocess.run(
        [str(arg) for arg in args], text=True, timeout=timeout,
        stdout=subprocess.DEVNULL if detached else subprocess.PIPE,
        stderr=subprocess.DEVNULL if detached else subprocess.PIPE,
    )
    if result.returncode:
        raise RuntimeError(f"{args}: {result.returncode}: {result.stdout} {result.stderr}")
    return result.stdout.strip() if result.stdout else ""


def counters(group):
    result = {}
    for name in ("memory.current", "memory.stat", "memory.events", "io.stat", "cpu.stat"):
        try:
            text = (group / name).read_text()
        except FileNotFoundError:
            result["removed"] = True
            continue
        if name == "memory.current":
            result[name] = int(text)
        elif name == "io.stat":
            result[name] = text
        else:
            result[name] = {k: int(v) for k, v in (line.split() for line in text.splitlines())}
    # Whole-VM leaf counters include checkpoint CLI writes charged outside guest.
    result["diskstats"] = Path("/proc/diskstats").read_text()
    return result


def sync_filesystem(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        library = ctypes.CDLL(None, use_errno=True)
        if library.syncfs(fd):
            raise OSError(ctypes.get_errno(), "syncfs failed")
    finally:
        os.close(fd)


def run(args):
    if os.geteuid() != 0:
        raise ValueError("isolated native qualification requires root")
    sync_filesystem(args.work_root)
    diskstats_before = Path("/proc/diskstats").read_text()
    root = args.work_root / f"tier-{args.mode}-{time.time_ns()}"
    root.mkdir(mode=0o700)
    bundle = root / "bundle"
    rootfs = bundle / "rootfs"
    for name in ("dev", "proc", "sys", "tmp"):
        (rootfs / name).mkdir(parents=True, exist_ok=True)
    shutil.copyfile(args.workload, rootfs / "workload")
    (rootfs / "workload").chmod(0o755)
    memory = root / "memory"
    memory.mkdir()
    ram = args.mode == "ram-park"
    if ram:
        command("mount", "-t", "tmpfs", "-o", "size=3g,noswap,mode=0700", "memory-tier", memory)
    (memory / "tier.sandbox-1").mkdir()
    namespace = f"memory-tier-{os.getpid()}"
    container = "tier"
    prefix = [args.runsc, f"--root={root / 'runtime'}"]
    common = [*prefix, f"--application-memory-file-dir={memory}", "--network=sandbox"]
    if ram:
        common.append("--application-memory-ram-backing")
    elif args.native_reflink:
        common.append("--application-memory-reflink-restore")
    report = {"mode": args.mode, "status": "failed", "samples": [],
              "runsc_sha256": hashlib.sha256(args.runsc.read_bytes()).hexdigest(),
              "workload_sha256": hashlib.sha256(args.workload.read_bytes()).hexdigest(),
              "heap_bytes": 1536*MIB, "dirty_bytes_per_turn": 384*MIB,
              "cold_restore": args.cold_restore,
              "whole_run_diskstats_before": diskstats_before,
              "scope": "native memory only; host XFS workspace; no product storage handoff"}
    try:
        command("ip", "netns", "add", namespace)
        command("ip", "netns", "exec", namespace, "ip", "link", "set", "lo", "up")
        config = Qualifier._gvisor_config(None, namespace, "tier.sandbox-1")
        config["linux"]["cgroupsPath"] = "/ucloud-memory-tier-" + root.name
        config["linux"]["resources"] = {
            "memory": {"limit": 2*1024**3, "swap": 2*1024**3},
            "cpu": {"quota": 100000, "period": 100000},
        }
        config["process"]["args"] = ["/workload", "server"]
        (bundle / "config.json").write_text(json.dumps(config))
        group = Path("/sys/fs/cgroup") / config["linux"]["cgroupsPath"].lstrip("/")
        command(*common, "create", f"--bundle={bundle}", container)
        command(*prefix, "start", container)
        deadline = time.monotonic() + 60
        while True:
            try:
                command(*prefix, "exec", container, "/workload", "ping")
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
        for cycle in range(args.cycles):
            before = counters(group)
            command(*prefix, "exec", container, "/workload", "dirty")
            start = time.monotonic()
            reclaimed_error = None
            image = root / f"checkpoint-{cycle}"
            if args.mode == "file-reclaim":
                # Explicit writeback prepares this live memory file for reclaim;
                # measured here, never hidden as pre-experiment cleanup.
                fd = os.open(memory / "tier.sandbox-1/application_memory.active", os.O_RDONLY)
                try:
                    os.fdatasync(fd)
                finally:
                    os.close(fd)
                writeback_seconds = time.monotonic() - start
                try:
                    (group / "memory.reclaim").write_text(f"{1536*MIB} swappiness=0")
                except OSError as exc:
                    if exc.errno != 11:
                        raise
                    reclaimed_error = "EAGAIN"
                release_seconds = time.monotonic() - start
                parked = counters(group)
                wake_seconds = 0.0
            else:
                image.mkdir()
                command(*common, "checkpoint", "--hibernate", f"--image-path={image}", container)
                # Ordinary backing checkpoint renames; durability is the owner
                # contract, so explicitly flush it as product publication does.
                for file in image.iterdir():
                    if file.is_file():
                        fd = os.open(file, os.O_RDONLY)
                        try:
                            os.fsync(fd)
                        finally:
                            os.close(fd)
                command(*prefix, "delete", "--force", container)
                release_seconds = time.monotonic() - start
                parked = counters(group)
                writeback_seconds = None
                if args.cold_restore:
                    # Only this dead runtime's complete immutable memory file:
                    # model pressure evicting checkpoint cache, never live pages.
                    fd = os.open(image / "application_memory.img", os.O_RDONLY)
                    try:
                        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                    finally:
                        os.close(fd)
                wake = time.monotonic()
                restore_image = image
                original = (image / "application_memory.img").stat()
                if args.prove_failure and cycle == 0:
                    if not args.native_reflink or ram:
                        raise ValueError("failure proof requires native file clone restore")
                    source = image / "application_memory.img"
                    with source.open("rb") as stream:
                        source_digest = hashlib.file_digest(stream, "sha256").hexdigest()
                    state = image / "checkpoint.img"
                    intact = state.read_bytes()
                    try:
                        state.write_bytes(b"injected invalid kernel metadata")
                        try:
                            command(*common, "restore", "--detach", "--background", "--start-paused", f"--image-path={image}", f"--bundle={bundle}", container)
                        except RuntimeError:
                            pass
                        else:
                            raise AssertionError("corrupt candidate unexpectedly restored")
                    finally:
                        command(*prefix, "delete", "--force", container)
                        state.write_bytes(intact)
                    with source.open("rb") as stream:
                        assert hashlib.file_digest(stream, "sha256").hexdigest() == source_digest
                    assert not (memory / "tier.sandbox-1/application_memory.active.restore").exists()
                    report["failed_candidate_preserved_source_and_cleaned_clone"] = True
                    wake = time.monotonic()
                if not ram and not args.native_reflink:
                    restore_image = root / f"candidate-{cycle}"
                    command("cp", "-a", "--reflink=always", image, restore_image)
                command(*common, "restore", "--detach", "--background", "--start-paused", f"--image-path={restore_image}", f"--bundle={bundle}", container)
                paused_restore_seconds = time.monotonic() - wake
                assert json.loads(command(*prefix, "state", container))["status"] == "paused"
                paused_candidate = counters(group)
                command(*prefix, "resume", container)
                wake_seconds = time.monotonic() - wake
                retained = (image / "application_memory.img").stat()
                assert (original.st_ino, original.st_size, original.st_mtime_ns, original.st_ctime_ns) == (
                    retained.st_ino, retained.st_size, retained.st_mtime_ns, retained.st_ctime_ns
                ), "restore changed retained checkpoint"
            restored = counters(group)
            ping = time.monotonic()
            command(*prefix, "exec", container, "/workload", "ping")
            first_ping_seconds = time.monotonic() - ping
            verify = time.monotonic()
            response = command(*prefix, "exec", container, "/workload", "verify")
            verify_seconds = time.monotonic() - verify
            after = counters(group)
            immutable_proof = None
            if args.prove_immutable and image.exists():
                source = image / "application_memory.img"
                with source.open("rb") as stream:
                    digest_before = hashlib.file_digest(stream, "sha256").hexdigest()
                command(*prefix, "exec", container, "/workload", "dirty")
                command(*prefix, "exec", container, "/workload", "verify")
                with source.open("rb") as stream:
                    digest_after = hashlib.file_digest(stream, "sha256").hexdigest()
                assert digest_before == digest_after, "guest mutation changed immutable source"
                immutable_proof = digest_after
            report["samples"].append({"cycle": cycle, "before": before, "parked": parked,
                "after": after, "release_seconds": release_seconds, "wake_seconds": wake_seconds,
                "restored_before_guest_touch": restored,
                "retained_checkpoint_after_guest_mutation": immutable_proof,
                "writeback_seconds": writeback_seconds, "first_ping_seconds": first_ping_seconds,
                "full_integrity_seconds": verify_seconds, "integrity": response,
                "reclaim_error": reclaimed_error})
            if args.mode != "file-reclaim":
                report["samples"][-1].update(
                    paused_restore_seconds=paused_restore_seconds,
                    paused_candidate=paused_candidate,
                )
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            if image.exists():
                shutil.rmtree(image)
        report["status"] = "passed"
    except BaseException as exc:
        report["error"] = repr(exc)
        raise
    finally:
        subprocess.run([str(x) for x in [*prefix, "delete", "--force", container]], capture_output=True)
        subprocess.run(["ip", "netns", "del", namespace], capture_output=True)
        if ram:
            command("umount", memory)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        subprocess.run(["umount", str(root / "runtime/null-netns")], capture_output=True)
        if report["status"] == "passed":
            shutil.rmtree(root)
        sync_filesystem(args.work_root)
        report["whole_run_diskstats_after"] = Path("/proc/diskstats").read_text()
        args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("runsc", "workload", "work-root", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--mode", choices=("ram-park", "file-park", "file-reclaim"), required=True)
    parser.add_argument("--cycles", type=int, default=4)
    parser.add_argument("--native-reflink", action="store_true")
    parser.add_argument("--prove-immutable", action="store_true",
                        help="extra mutation/hash proof outside timing/device-byte sample")
    parser.add_argument("--prove-failure", action="store_true")
    parser.add_argument("--cold-restore", action="store_true")
    run(parser.parse_args())
