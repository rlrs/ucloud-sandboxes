#!/usr/bin/env python3
"""Measure the durability cost omitted by the metadata-only park benchmark."""

from __future__ import annotations

import argparse
import json
import mmap
import os
from pathlib import Path
import platform
import shutil
import time


MIB = 1024 * 1024


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/tmp/hibernate-fsync"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--sizes-mib", type=int, nargs="+", default=(256, 1024, 4096))
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    if args.rounds < 1 or any(size < 1 for size in args.sizes_mib):
        parser.error("sizes and rounds must be positive")

    args.root.mkdir(mode=0o700, parents=True, exist_ok=True)
    samples: list[dict[str, int | float]] = []
    for size_mib in args.sizes_mib:
        length = size_mib * MIB
        for round_number in range(1, args.rounds + 1):
            path = args.root / f"{size_mib}m-{round_number}.img"
            descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                os.ftruncate(descriptor, length)
                mapping = mmap.mmap(descriptor, length, access=mmap.ACCESS_WRITE)
                dirty_started = time.monotonic_ns()
                # One byte per page makes the full sparse extent physically
                # dirty while avoiding a Python-sized copy buffer.
                mapping[0:length:4096] = b"\xa5" * (length // 4096)
                dirty_ns = time.monotonic_ns() - dirty_started
                seal_started = time.monotonic_ns()
                mapping.flush()
                os.fsync(descriptor)
                seal_ns = time.monotonic_ns() - seal_started
                mapping.close()
                info = os.fstat(descriptor)
            finally:
                os.close(descriptor)
            path.unlink()
            samples.append(
                {
                    "size_mib": size_mib,
                    "round": round_number,
                    "dirty_ms": dirty_ns / 1_000_000,
                    "seal_ms": seal_ns / 1_000_000,
                    "seal_mib_per_second": size_mib / (seal_ns / 1_000_000_000),
                    "allocated_mib": info.st_blocks * 512 // MIB,
                }
            )

    usage = shutil.disk_usage(args.root)
    result = {
        "schema": 1,
        "hostname": platform.node(),
        "kernel": platform.release(),
        "filesystem": os.statvfs(args.root).f_fsid,
        "disk_total_mib": usage.total // MIB,
        "disk_free_mib": usage.free // MIB,
        "samples": samples,
    }
    payload = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")


if __name__ == "__main__":
    main()
