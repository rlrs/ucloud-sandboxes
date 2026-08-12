#!/usr/bin/env python3
"""Measure repeated managed-process park/resume correctness through the gateway."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import statistics
import time
from typing import Any, Mapping
from uuid import uuid4

import aiohttp
from ucloud_sandboxes_sdk import (
    AsyncSandboxClient,
    Image,
    SandboxSecuritySpec,
    SandboxSpec,
)


COUNTER_PROGRAM = r'''#!/usr/bin/env python3
import hashlib
import json
import os
from pathlib import Path
import time

path = Path("/workspace/counter.json")
counter = 0
if path.exists():
    counter = int(json.loads(path.read_text())["counter"])
while True:
    counter += 1
    text = str(counter)
    record = {
        "counter": counter,
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
    }
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(record, sort_keys=True))
    os.replace(temporary, path)
    time.sleep(0.02)
'''


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway-url", default="http://127.0.0.1:8090")
    parser.add_argument("--gateway-token-file", type=Path, required=True)
    parser.add_argument("--image", default="python:3.13-slim-bookworm")
    parser.add_argument("--cycles", type=int, default=20)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def read_token(path: Path) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError(f"empty token file: {path}")
    return value


async def gateway_json(
    session: aiohttp.ClientSession,
    gateway_url: str,
    token: str,
    method: str,
    path: str,
    *,
    payload: object | None = None,
    attempts: int = 1,
) -> tuple[dict[str, Any], int]:
    headers = {
        "X-UCloud-Sandbox-Token": token,
        "Accept": "application/json",
    }
    last_status = 0
    last_body: object = None
    for attempt in range(attempts):
        async with session.request(
            method,
            gateway_url.rstrip("/") + path,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=180),
        ) as response:
            body = await response.read()
            decoded = json.loads(body.decode()) if body else {}
            if response.status < 300:
                if not isinstance(decoded, dict):
                    raise RuntimeError("gateway returned non-object JSON")
                return decoded, attempt
            last_status = response.status
            last_body = decoded
            if response.status not in {409, 503} or attempt + 1 >= attempts:
                break
        await asyncio.sleep(0.05)
    raise RuntimeError(
        f"gateway {method} {path} failed ({last_status}): {last_body}"
    )


def generation(record: Mapping[str, Any]) -> int:
    for source in (record, record.get("sandbox"), record.get("status")):
        if isinstance(source, Mapping):
            value = source.get("generation")
            if isinstance(value, int) and value > 0:
                return value
    raise RuntimeError("sandbox record omitted generation")


async def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.cycles < 1:
        raise ValueError("cycles must be positive")
    token = read_token(args.gateway_token_file)
    suffix = uuid4().hex[:10]
    sandbox_id = f"park-bench-{suffix}"
    job_id = f"counter-{suffix}"
    cycles: list[dict[str, Any]] = []
    started = time.monotonic()

    async with AsyncSandboxClient(
        args.gateway_url,
        api_token=token,
        timeout_seconds=30,
    ) as client, aiohttp.ClientSession() as session:
        handle = await client.create_sandbox(
            SandboxSpec(
                id=sandbox_id,
                image=Image.from_registry(args.image),
                memory_mb=1024,
                cpus=1.0,
                disk_mb=2048,
                network="bridge",
                ttl_seconds=900,
                parkable=True,
                managed_process=True,
                security=SandboxSecuritySpec(user="0:0"),
            ),
            request_timeout_seconds=180,
        )
        try:
            record = await client.get_sandbox(sandbox_id)
            if record is None:
                raise RuntimeError("created sandbox disappeared")
            sandbox_generation = 0
            for source in (handle.record, handle.create_response, record):
                try:
                    sandbox_generation = generation(source)
                    break
                except RuntimeError:
                    continue
            if sandbox_generation < 1:
                raise RuntimeError("create response and inventory omitted generation")
            await handle.upload_file("/workspace/counter.py", COUNTER_PROGRAM)
            start, _ = await gateway_json(
                session,
                args.gateway_url,
                token,
                "POST",
                f"/v1/sandboxes/{sandbox_id}/jobs",
                payload={
                    "job_id": job_id,
                    "argv": ["python", "/workspace/counter.py"],
                    "cwd": "/workspace",
                    "env": {},
                },
            )
            baseline_job = start["job"]
            baseline_pid = int(baseline_job["pid"])
            baseline_spec = str(baseline_job["spec_sha256"])

            async def read_counter(after: int, timeout: float = 10) -> int:
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    try:
                        raw = await handle.download_file("/workspace/counter.json")
                        item = json.loads(raw.decode())
                        value = int(item["counter"])
                        expected = hashlib.sha256(str(value).encode()).hexdigest()
                        if item.get("sha256") != expected:
                            raise RuntimeError("counter checksum mismatch")
                        if value > after:
                            return value
                    except Exception:
                        pass
                    await asyncio.sleep(0.02)
                raise TimeoutError(f"counter did not advance beyond {after}")

            current_counter = await read_counter(0)
            for index in range(1, args.cycles + 1):
                park_started = time.monotonic()
                parked, park_retries = await gateway_json(
                    session,
                    args.gateway_url,
                    token,
                    "POST",
                    f"/v1/sandboxes/{sandbox_id}/park",
                    payload={
                        "generation": sandbox_generation,
                        "operation_id": f"park-benchmark:{suffix}:{index}",
                    },
                    attempts=101,
                )
                park_seconds = time.monotonic() - park_started
                parked_job, _ = await gateway_json(
                    session,
                    args.gateway_url,
                    token,
                    "GET",
                    f"/v1/sandboxes/{sandbox_id}/jobs/{job_id}",
                )

                wake_started = time.monotonic()
                woke, wake_retries = await gateway_json(
                    session,
                    args.gateway_url,
                    token,
                    "POST",
                    f"/v1/sandboxes/{sandbox_id}/wake",
                    payload={
                        "generation": sandbox_generation,
                        "operation_id": f"wake-benchmark:{suffix}:{index}",
                    },
                    attempts=101,
                )
                wake_seconds = time.monotonic() - wake_started
                next_counter = await read_counter(current_counter)
                running_job, _ = await gateway_json(
                    session,
                    args.gateway_url,
                    token,
                    "GET",
                    f"/v1/sandboxes/{sandbox_id}/jobs/{job_id}",
                )
                parked_record = parked.get("sandbox", {})
                woke_record = woke.get("sandbox", {})
                parked_process = parked_job.get("job", {})
                running_process = running_job.get("job", {})
                cycles.append(
                    {
                        "cycle": index,
                        "park_seconds": park_seconds,
                        "wake_seconds": wake_seconds,
                        "park_retries": park_retries,
                        "wake_retries": wake_retries,
                        "parked_state": parked_record.get("state"),
                        "woke_state": woke_record.get("state"),
                        "counter_before": current_counter,
                        "counter_after": next_counter,
                        "parked_pid": parked_process.get("pid"),
                        "running_pid": running_process.get("pid"),
                        "parked_spec_sha256": parked_process.get("spec_sha256"),
                        "running_spec_sha256": running_process.get("spec_sha256"),
                    }
                )
                current_counter = next_counter

            park_values = [item["park_seconds"] for item in cycles]
            wake_values = [item["wake_seconds"] for item in cycles]
            correctness = {
                "every_park_reached_parked": all(
                    item["parked_state"] == "parked" for item in cycles
                ),
                "every_wake_reached_running": all(
                    item["woke_state"] == "running" for item in cycles
                ),
                "counter_advanced_after_every_wake": all(
                    item["counter_after"] > item["counter_before"]
                    for item in cycles
                ),
                "process_identity_preserved": all(
                    item["parked_pid"] == baseline_pid
                    and item["running_pid"] == baseline_pid
                    and item["parked_spec_sha256"] == baseline_spec
                    and item["running_spec_sha256"] == baseline_spec
                    for item in cycles
                ),
            }
            return {
                "ok": all(correctness.values()),
                "sandbox_id": sandbox_id,
                "generation": sandbox_generation,
                "cycles": cycles,
                "correctness": correctness,
                "elapsed_seconds": time.monotonic() - started,
                "performance": {
                    "park_min_seconds": min(park_values),
                    "park_median_seconds": statistics.median(park_values),
                    "park_max_seconds": max(park_values),
                    "wake_min_seconds": min(wake_values),
                    "wake_median_seconds": statistics.median(wake_values),
                    "wake_max_seconds": max(wake_values),
                    "total_lifecycle_retries": sum(
                        item["park_retries"] + item["wake_retries"]
                        for item in cycles
                    ),
                },
                "final_counter": current_counter,
            }
        finally:
            await handle.delete()


def main() -> None:
    args = parse_args()
    result = asyncio.run(run(args))
    encoded = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    if not result["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
