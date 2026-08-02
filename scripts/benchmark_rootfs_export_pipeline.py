#!/usr/bin/env python3
"""Deterministic benchmark for rootfs export concurrency and warm bypass."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import statistics
from tempfile import TemporaryDirectory
from threading import Lock
import time

from ucloud_sandboxes.direct_warden import CommandResult
from ucloud_sandboxes.image_rootfs import DockerRootfsStore


class BenchmarkRunner:
    def run(self, argv, *, timeout):
        del timeout
        command = tuple(str(item) for item in argv)
        if command[:3] == ("docker", "image", "inspect"):
            digest = hashlib.sha256(command[-1].encode("utf-8")).hexdigest()
            return CommandResult(
                command,
                0,
                json.dumps([{"Id": f"sha256:{digest}", "Config": {}}]),
            )
        if command[:2] == ("docker", "ps"):
            return CommandResult(command, 0, "")
        if command[:2] == ("docker", "create"):
            container_id = hashlib.sha256(command[-1].encode("utf-8")).hexdigest()
            return CommandResult(command, 0, container_id + "\n")
        if command[:2] == ("docker", "export"):
            output = Path(
                next(
                    item.split("=", 1)[1]
                    for item in command
                    if item.startswith("--output=")
                )
            )
            output.write_bytes(b"benchmark")
        return CommandResult(command, 0)


class DelayedExtractor:
    def __init__(self, delay_seconds: float) -> None:
        self.delay_seconds = delay_seconds
        self._guard = Lock()
        self._active = 0
        self.max_active = 0

    def extract(self, archive: Path, destination: Path) -> None:
        if not archive.is_file():
            raise RuntimeError("benchmark export archive is absent")
        with self._guard:
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        try:
            time.sleep(self.delay_seconds)
            (destination / "payload").write_bytes(b"rootfs")
        finally:
            with self._guard:
                self._active -= 1


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * fraction))
    return ordered[index]


def run(images: int, concurrency: int, delay_seconds: float) -> dict[str, object]:
    refs = [f"benchmark:image-{index}" for index in range(images)]
    with TemporaryDirectory() as raw:
        extractor = DelayedExtractor(delay_seconds)
        store = DockerRootfsStore(
            (Path(raw) / "cache").resolve(),
            runner=BenchmarkRunner(),
            extractor=extractor,
            max_concurrent_exports=concurrency,
        )

        cold_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=images) as pool:
            list(pool.map(store.materialize, refs))
        cold_ms = (time.perf_counter() - cold_started) * 1000

        def warm(ref: str) -> float:
            started = time.perf_counter()
            store.materialize(ref)
            return (time.perf_counter() - started) * 1000

        warm_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=images) as pool:
            warm_values = list(pool.map(warm, refs))
        warm_ms = (time.perf_counter() - warm_started) * 1000

    serialized_baseline_ms = images * delay_seconds * 1000
    return {
        "schema": 1,
        "images": images,
        "max_concurrent_exports": concurrency,
        "synthetic_extract_delay_ms": delay_seconds * 1000,
        "cold_makespan_ms": cold_ms,
        "serialized_baseline_ms": serialized_baseline_ms,
        "cold_speedup_vs_serialized": serialized_baseline_ms / cold_ms,
        "observed_max_concurrent_exports": extractor.max_active,
        "warm_makespan_ms": warm_ms,
        "warm_p50_ms": statistics.median(warm_values),
        "warm_p95_ms": percentile(warm_values, 0.95),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--images", type=int, default=32)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--extract-delay", type=float, default=0.1)
    args = parser.parse_args()
    if args.images < 1 or args.concurrency < 1 or args.extract_delay < 0:
        parser.error("images/concurrency must be positive and delay non-negative")
    print(
        json.dumps(
            run(args.images, args.concurrency, args.extract_delay),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
