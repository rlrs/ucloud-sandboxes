#!/usr/bin/env python3
"""EROFS metadata walker on a real rootfs, erofs-utils 1.9 (scratch; root on the spike VM).

For each image variant: walk it, then prove completeness four ways at 4 KiB
block granularity. Every block read that is not a regular-file data extent
(dump.erofs -e) must be in the walker's ranges, for
  * a kernel EROFS mount over a logging NBD export, metadata-only traversal
    (readdir, lstat, readlink, listxattr, getxattr, statfs);
  * the same with every regular file read in full;
  * fsck.erofs --extract (strace);
  * dump.erofs --nid -e on a sample of inodes (strace).
Finally, everything outside metadata plus data extents is overwritten, and a
kernel mount of that image must show the identical tree, stat, xattrs and
file contents, and fsck must pass.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import time
from threading import Lock
from types import SimpleNamespace

sys.path.insert(0, "/root/qual/ucloud-sandboxes")
from ucloud_sandboxes.environment_builder import WHOLE_IMAGE_EXCLUDED  # noqa: E402
from ucloud_sandboxes.environment_nbd import EnvironmentReadWorkers, ReadOnlyEnvironmentDevice  # noqa: E402
from ucloud_sandboxes.erofs_metadata import metadata_ranges, walk  # noqa: E402
from tests.test_erofs_metadata import (  # noqa: E402
    _data_extents, _dump_all, _ranges_blocks, _subtract, _traced, FSCK)

BLOCK = 4096
NULL_UUID = "00000000-0000-0000-0000-000000000000"
EXCLUDE = "--exclude-regex=^(" + "|".join(sorted(WHOLE_IMAGE_EXCLUDED)) + ")$"
VARIANTS = {
    "production-T0-lz4": ["-T", "0", "-U", NULL_UUID, "-zlz4", EXCLUDE],
    "production-T0-mkfs-time-lz4": ["-T", "0", "--mkfs-time", "-U", NULL_UUID, "-zlz4", EXCLUDE],
    "T0-mkfs-time-uncompressed": ["-T", "0", "--mkfs-time", "-U", NULL_UUID, EXCLUDE],
    "production-T0-mkfs-time-lz4-MZ": ["-T", "0", "--mkfs-time", "-U", NULL_UUID, "-zlz4", "--MZ", EXCLUDE],
}


class LoggingCache:
    """Serves the image file and records every NBD read range."""

    def __init__(self, image):
        self.fd = os.open(image, os.O_RDONLY)
        self.reads, self.lock = [], Lock()

    def read(self, component, offset, length, cancel=None):
        with self.lock:
            self.reads.append((offset, offset + length))
        return os.pread(self.fd, length, offset)

    def close(self):
        os.close(self.fd)


def free_nbd():
    for index in range(1024):
        if not Path(f"/sys/block/nbd{index}/pid").exists():
            yield Path(f"/dev/nbd{index}")


def traverse(root, *, read_files):
    """Tree snapshot: path -> (mode, uid, gid, size, nlink, rdev, target, xattrs, sha256)."""
    snapshot = {}
    os.statvfs(root)
    for directory, names, files in os.walk(root):
        for name in [None, *names, *files]:
            path = Path(directory) if name is None else Path(directory) / name
            if name is None and path != root:
                continue
            info = os.lstat(path)
            target = os.readlink(path) if stat.S_ISLNK(info.st_mode) else None
            xattrs = {key: os.getxattr(path, key, follow_symlinks=False).hex()
                      for key in sorted(os.listxattr(path, follow_symlinks=False))}
            digest = None
            if read_files and stat.S_ISREG(info.st_mode):
                with open(path, "rb") as source:
                    digest = hashlib.sha256(source.read()).hexdigest()
            # Directory nlink and size depend on the filesystem; ignore for dirs.
            regular = not stat.S_ISDIR(info.st_mode)
            snapshot[str(path.relative_to(root))] = (
                info.st_mode, info.st_uid, info.st_gid, info.st_size if regular else None,
                info.st_nlink if regular else None, info.st_rdev, target, xattrs, digest)
    return snapshot


def kernel_traversal(image, mountpoint, *, read_files):
    subprocess.run(["sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"], check=True)
    cache = LoggingCache(image)
    component = SimpleNamespace(image_size=image.stat().st_size, authenticate=lambda keys: None)
    workers = EnvironmentReadWorkers(4)
    device = None
    for candidate in free_nbd():
        try:
            device = ReadOnlyEnvironmentDevice(candidate, component, cache, workers, trusted_keys={})
            break
        except OSError:
            continue
    mountpoint.mkdir(exist_ok=True)
    try:
        started = time.monotonic()
        subprocess.run(["mount", "-t", "erofs", "-o", "ro", str(device.path), str(mountpoint)], check=True)
        mounted = time.monotonic()
        try:
            # A separate process, as in production: the exporting process
            # never touches its own mounts (an in-process os.walk stalled).
            output = mountpoint.parent / "snapshot.json"
            subprocess.run([sys.executable, __file__, "--traverse", str(mountpoint), "--snapshot", str(output),
                            *(["--read-files"] if read_files else [])], check=True, timeout=600)
            snapshot = {path: tuple(value) for path, value in json.loads(output.read_text()).items()}
            output.unlink()
            finished = time.monotonic()
        finally:
            subprocess.run(["umount", str(mountpoint)], check=True)
    finally:
        device.close()
        workers.close()
        cache.close()
    return cache.reads, snapshot, {"mount_seconds": mounted - started, "traverse_seconds": finished - mounted}


def loop_snapshot(image, mountpoint):
    mountpoint.mkdir(exist_ok=True)
    subprocess.run(["mount", "-t", "erofs", "-o", "ro,loop", str(image), str(mountpoint)], check=True)
    try:
        return {path: tuple(value) for path, value in
                json.loads(json.dumps(traverse(mountpoint, read_files=True))).items()}
    finally:
        subprocess.run(["umount", str(mountpoint)], check=True)


def uncovered(reads, extents, metadata):
    residual = _subtract(reads, extents)
    missing = sorted(_ranges_blocks(residual) - _ranges_blocks(metadata.ranges))
    return {"reads": len(reads), "read_bytes": sum(end - start for start, end in reads),
            "non_data_blocks": len(_ranges_blocks(residual)), "uncovered_blocks": missing[:50],
            "uncovered_count": len(missing)}


def qualify(name, options, source, work):
    image = work / f"{name}.erofs"
    started = time.monotonic()
    subprocess.run(["mkfs.erofs", *options, str(image), str(source)], check=True, capture_output=True)
    report = {"options": options, "mkfs_seconds": time.monotonic() - started,
              "image_bytes": image.stat().st_size,
              "sha256": hashlib.sha256(image.read_bytes()).hexdigest()}
    # Determinism: a second build of the same tree.
    again = work / f"{name}.again.erofs"
    subprocess.run(["mkfs.erofs", *options, str(again), str(source)], check=True, capture_output=True)
    report["rebuild_identical"] = again.read_bytes() == image.read_bytes()
    again.unlink()
    inodes = []
    started = time.monotonic()
    metadata = metadata_ranges(image, on_inode=lambda *item: inodes.append(item))
    report["walk_seconds"] = time.monotonic() - started
    layouts = {}
    for _, kind, layout, *_ in inodes:
        key = f"{stat.filemode(kind)[0]}{layout}"
        layouts[key] = layouts.get(key, 0) + 1
    report.update(inodes=metadata.inodes, directories=metadata.directories, symlinks=metadata.symlinks,
                  metadata_bytes=metadata.metadata_bytes, ranges=len(metadata.ranges),
                  metadata_blocks=len(_ranges_blocks(metadata.ranges)),
                  hint_chunks_256k=len(metadata.chunk_bytes(256 * 1024)), layouts=layouts)
    proofs = {}
    meta_reads, meta_snapshot, meta_timing = kernel_traversal(image, work / "mnt", read_files=False)
    full_reads, full_snapshot, full_timing = kernel_traversal(image, work / "mnt", read_files=True)
    started = time.monotonic()
    sections, _ = _dump_all(image, [nid for nid, kind, *_ in inodes if kind == stat.S_IFREG])
    extents = _data_extents(sections, inodes)
    report["dump_extents_seconds"] = time.monotonic() - started
    report["data_extents"] = len(extents)
    report["data_extent_bytes"] = sum(end - start for start, end in extents)
    if [nid for nid, body in sections.items() if "@@failed" in body]:
        raise RuntimeError("dump.erofs failed on some inodes")
    proofs["kernel_metadata_traversal"] = uncovered(meta_reads, extents, metadata) | meta_timing | {
        "strict_uncovered_without_data_subtraction": len(
            _ranges_blocks(meta_reads) - _ranges_blocks(metadata.ranges))}
    proofs["kernel_full_read"] = uncovered(full_reads, extents, metadata) | full_timing
    reads, _ = _traced([FSCK, "--extract", str(image)], image, work / f"{name}.fsck.trace")
    proofs["fsck_extract"] = uncovered(reads, extents, metadata)
    sample = [nid for index, (nid, kind, layout, *_) in enumerate(inodes)
              if kind in (stat.S_IFDIR, stat.S_IFLNK) or not index % 16]
    _, reads = _dump_all(image, sample, trace_log=work / f"{name}.dump.trace")
    proofs["dump_sample"] = uncovered(reads, extents, metadata) | {"nids": len(sample)}
    report["proofs"] = proofs
    # Overwrite everything outside metadata and data extents.
    original = image.read_bytes()
    overwritten = bytearray(b"\x5a" * len(original))
    for start, end in [*metadata.ranges, *extents]:
        overwritten[start:end] = original[start:end]
    garbage = work / f"{name}.overwritten.erofs"
    garbage.write_bytes(overwritten)
    changed_bytes = sum(1 for index in range(0, len(original), BLOCK)
                        if original[index:index + BLOCK] != overwritten[index:index + BLOCK]) * BLOCK
    fsck = subprocess.run([FSCK, "--extract", str(garbage)], capture_output=True, text=True)
    after = loop_snapshot(garbage, work / "mnt")
    report["overwrite"] = {
        "changed_block_bytes": changed_bytes, "fsck_returncode": fsck.returncode,
        "fsck_stderr": fsck.stderr[-500:],
        "kernel_tree_identical": after == full_snapshot,
        "differences": sorted(path for path in set(after) | set(full_snapshot)
                              if after.get(path) != full_snapshot.get(path))[:20],
        "walker_identical": walk(bytes(overwritten)).ranges == metadata.ranges,
        "metadata_snapshot_matches_full": all(
            full_snapshot[path][:8] == meta_snapshot[path][:8] for path in full_snapshot),
        "entries": len(full_snapshot)}
    garbage.unlink()
    image.unlink()
    for trace in work.glob(f"{name}.*.trace"):
        trace.unlink()
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=Path("/srv/spike/rootfs"))
    parser.add_argument("--work", type=Path, default=Path("/var/lib/rl-spike/qual-walker"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--variants", default=",".join(VARIANTS))
    parser.add_argument("--traverse", type=Path)
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--read-files", action="store_true")
    args = parser.parse_args()
    if args.traverse:
        args.snapshot.write_text(json.dumps(traverse(args.traverse, read_files=args.read_files)))
        return
    shutil.rmtree(args.work, ignore_errors=True)
    args.work.mkdir(parents=True)
    version = subprocess.run(["mkfs.erofs", "-V"], capture_output=True, text=True).stdout.strip()
    result = {"mkfs": version, "kernel": os.uname().release, "source": str(args.source), "variants": {}}
    for name in args.variants.split(","):
        try:
            result["variants"][name] = qualify(name, VARIANTS[name], args.source, args.work)
        except Exception as exc:  # Record and continue with the next variant.
            import traceback
            result["variants"][name] = {"error": repr(exc), "traceback": traceback.format_exc()}
        args.output.write_text(json.dumps(result, indent=1) + "\n")
        print(name, json.dumps(result["variants"][name].get("proofs", result["variants"][name]))[:2000],
              flush=True)
    shutil.rmtree(args.work, ignore_errors=True)


if __name__ == "__main__":
    main()
