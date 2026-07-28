#!/usr/bin/env python3
"""Benchmark concurrent lifecycle operations through the deployed node API."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import time

from qualify_direct_node import _delete, _exec, _request, _sandbox


def _percentile(samples: list[float], percentile: float) -> float:
    ordered = sorted(samples)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return round(ordered[index], 3)


def _summary(samples: list[float]) -> dict[str, float]:
    return {
        "min": round(min(samples), 3),
        "p50": _percentile(samples, 0.50),
        "p95": _percentile(samples, 0.95),
        "max": round(max(samples), 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8090")
    parser.add_argument("--count", type=int, default=24)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--image", default="busybox:latest")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.count < 1 or args.concurrency < 1:
        raise ValueError("count and concurrency must be positive")

    prefix = f"direct-api-{time.time_ns()}"
    sandbox_ids = [f"{prefix}-{index}" for index in range(args.count)]
    _, pull_ms = _request(
        args.base_url,
        "/v1/images/pull",
        method="POST",
        payload={"image": args.image, "id": f"{prefix}-image"},
    )

    def create(sandbox_id: str) -> float:
        _, create_ms = _request(
            args.base_url,
            "/v1/sandboxes",
            method="POST",
            payload={
                "id": sandbox_id,
                "image": args.image,
                "command": ["/bin/sleep", "86400"],
                "cpus": 0.25,
                "memory_mb": 128,
                "disk_mb": 128,
                "parkable": True,
                "security": {"user": "0:0", "init": False},
            },
        )
        marker = f"{sandbox_id}\n".encode("ascii")
        _request(
            args.base_url,
            f"/v1/sandboxes/{sandbox_id}/files?path=/workspace/marker",
            method="PUT",
            body=marker,
            headers={"Content-Type": "application/octet-stream"},
        )
        return create_ms

    def park(sandbox_id: str) -> float:
        payload, elapsed_ms = _request(
            args.base_url,
            f"/v1/sandboxes/{sandbox_id}/park",
            method="POST",
            payload={"operation_id": f"park-{sandbox_id}"},
        )
        if payload["sandbox"]["state"] != "parked":
            raise RuntimeError(f"{sandbox_id} did not park")
        return elapsed_ms

    def wake(sandbox_id: str) -> dict[str, float]:
        result, elapsed_ms = _exec(
            args.base_url,
            sandbox_id,
            ["/bin/cat", "/workspace/marker"],
        )
        if result["stdout"] != f"{sandbox_id}\n":
            raise RuntimeError(f"{sandbox_id} restored the wrong marker")
        timings = dict(result["timings_ms"])
        timings["client_total"] = elapsed_ms
        return timings

    def delete(sandbox_id: str) -> float:
        record = _sandbox(args.base_url, sandbox_id)
        if record is None:
            raise RuntimeError(f"{sandbox_id} disappeared before delete")
        return _delete(args.base_url, record)

    def cleanup() -> None:
        payload, _ = _request(args.base_url, "/v1/sandboxes")
        assert isinstance(payload, dict)
        records = [
            item
            for item in payload.get("sandboxes", [])
            if item.get("id", "").startswith(prefix)
        ]
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            list(pool.map(lambda item: _delete(args.base_url, item), records))

    started = time.monotonic()
    try:
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            phase = time.monotonic()
            create_ms = list(pool.map(create, sandbox_ids))
            create_makespan_ms = (time.monotonic() - phase) * 1000
            phase = time.monotonic()
            park_ms = list(pool.map(park, sandbox_ids))
            park_makespan_ms = (time.monotonic() - phase) * 1000
            parked_heartbeat, _ = _request(args.base_url, "/v1/heartbeat")
            phase = time.monotonic()
            wake_timings = list(pool.map(wake, sandbox_ids))
            wake_makespan_ms = (time.monotonic() - phase) * 1000
            phase = time.monotonic()
            delete_ms = list(pool.map(delete, sandbox_ids))
            delete_makespan_ms = (time.monotonic() - phase) * 1000
    except BaseException:
        try:
            cleanup()
        except BaseException:
            pass
        raise
    total_ms = (time.monotonic() - started) * 1000

    remaining, _ = _request(args.base_url, "/v1/sandboxes")
    remaining_ids = {
        item["id"] for item in remaining["sandboxes"] if item["id"].startswith(prefix)
    }
    if remaining_ids:
        raise RuntimeError(f"benchmark leaked sandboxes: {sorted(remaining_ids)}")

    heartbeat = parked_heartbeat["heartbeat"]
    wake_timing_names = (
        "client_completion_wait",
        "client_events",
        "client_start_request",
        "client_total",
        "poll_count",
        "server_start",
    ) + tuple(
        sorted(
            {
                name
                for item in wake_timings
                for name in item
                if name.startswith("server_")
            }
        )
    )
    result = {
        "concurrency": args.concurrency,
        "count": args.count,
        "image_pull_ms": round(pull_ms, 3),
        "parked_heartbeat": {
            "active_sandboxes": heartbeat["active_sandboxes"],
            "used_resources": heartbeat["used_resources"],
        },
        "phases_ms": {
            "create": _summary(create_ms),
            "delete": _summary(delete_ms),
            "park": _summary(park_ms),
            "wake_exec": {
                name: _summary([item[name] for item in wake_timings])
                for name in wake_timing_names
            },
        },
        "phase_makespan_ms": {
            "create": round(create_makespan_ms, 3),
            "delete": round(delete_makespan_ms, 3),
            "park": round(park_makespan_ms, 3),
            "wake_exec": round(wake_makespan_ms, 3),
        },
        "total_ms": round(total_ms, 3),
    }
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
