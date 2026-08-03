#!/usr/bin/env python3
"""Measure cold Docker pull makespan at a bounded distinct-image concurrency."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import subprocess
import time
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--concurrency", type=int, required=True)
    parser.add_argument("--image", action="append", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    return parser.parse_args()


def pull_image(
    docker: str,
    image: str,
    *,
    benchmark_started: float,
    timeout_seconds: float,
) -> dict[str, Any]:
    started = time.monotonic()
    try:
        result = subprocess.run(
            [docker, "pull", image],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        error = "\n".join(result.stderr.strip().splitlines()[-8:])
        returncode = result.returncode
    except subprocess.TimeoutExpired as exc:
        error = f"pull timed out after {timeout_seconds:g}s: {exc}"
        returncode = 124
    finished = time.monotonic()
    return {
        "image": image,
        "returncode": returncode,
        "started_ms": round((started - benchmark_started) * 1000),
        "duration_ms": round((finished - started) * 1000),
        "error": error,
    }


def image_size(docker: str, image: str) -> int | None:
    result = subprocess.run(
        [docker, "image", "inspect", "--format", "{{.Size}}", image],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    try:
        return max(0, int(result.stdout.strip()))
    except ValueError:
        return None


def main() -> int:
    args = parse_args()
    if args.concurrency < 1:
        raise SystemExit("--concurrency must be positive")
    images = tuple(dict.fromkeys(str(image) for image in args.image if image))
    if not images:
        raise SystemExit("at least one non-empty --image is required")

    benchmark_started = time.monotonic()
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {
            executor.submit(
                pull_image,
                args.docker,
                image,
                benchmark_started=benchmark_started,
                timeout_seconds=args.timeout_seconds,
            ): image
            for image in images
        }
        for future in as_completed(futures):
            results.append(future.result())
    makespan_ms = round((time.monotonic() - benchmark_started) * 1000)

    for result in results:
        result["size_bytes"] = image_size(args.docker, str(result["image"]))
    results.sort(key=lambda item: images.index(str(item["image"])))
    successful_bytes = sum(
        int(result["size_bytes"] or 0)
        for result in results
        if result["returncode"] == 0
    )
    print(
        json.dumps(
            {
                "schema": 1,
                "concurrency": args.concurrency,
                "image_count": len(images),
                "successful": sum(
                    result["returncode"] == 0 for result in results
                ),
                "failed": sum(result["returncode"] != 0 for result in results),
                "makespan_ms": makespan_ms,
                "successful_image_bytes": successful_bytes,
                "effective_image_mib_per_second": round(
                    successful_bytes / (1024 * 1024) / max(0.001, makespan_ms / 1000),
                    3,
                ),
                "results": results,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if all(result["returncode"] == 0 for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
