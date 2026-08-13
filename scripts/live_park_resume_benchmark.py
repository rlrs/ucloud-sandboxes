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


COUNTER_PROGRAM = r"""#!/usr/bin/env python3
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
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway-url", default="http://127.0.0.1:8090")
    parser.add_argument("--gateway-token-file", type=Path, required=True)
    parser.add_argument(
        "--sandbox-api-token-file",
        type=Path,
        help=(
            "sandbox-scoped SDK credential; defaults to the gateway token for "
            "backward compatibility"
        ),
    )
    parser.add_argument("--image", default="python:3.13-slim-bookworm")
    parser.add_argument("--cycles", type=int, default=20)
    parser.add_argument("--detach-before-wake", action="store_true")
    parser.add_argument(
        "--wake-via-exec",
        action="store_true",
        help=(
            "wake by starting a no-op exec instead of using the operator wake "
            "route, and retain the node's phase timings"
        ),
    )
    parser.add_argument("--force-different-worker", action="store_true")
    parser.add_argument("--node-control-token-file", type=Path)
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
    raise RuntimeError(f"gateway {method} {path} failed ({last_status}): {last_body}")


async def node_json(
    session: aiohttp.ClientSession,
    node_url: str,
    token: str,
    method: str,
    path: str,
    *,
    payload: object,
) -> dict[str, Any]:
    async with session.request(
        method,
        node_url.rstrip("/") + path,
        json=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        timeout=aiohttp.ClientTimeout(total=30),
    ) as response:
        body = await response.read()
        decoded = json.loads(body.decode()) if body else {}
        if response.status >= 300:
            raise RuntimeError(
                f"node {method} {path} failed ({response.status}): {decoded}"
            )
        if not isinstance(decoded, dict):
            raise RuntimeError("node returned non-object JSON")
        return decoded


async def cached_sandbox_route(
    session: aiohttp.ClientSession,
    gateway_url: str,
    token: str,
    sandbox_id: str,
) -> dict[str, Any]:
    payload, _ = await gateway_json(
        session,
        gateway_url,
        token,
        "GET",
        "/v1/sandboxes",
    )
    sandboxes = payload.get("sandboxes")
    if not isinstance(sandboxes, list):
        raise RuntimeError("cached sandbox inventory is missing")
    for item in sandboxes:
        if isinstance(item, dict) and item.get("id") == sandbox_id:
            return item
    raise RuntimeError("sandbox is absent from cached routing inventory")


async def wait_for_gateway_drain_state(
    session: aiohttp.ClientSession,
    gateway_url: str,
    token: str,
    node_id: str,
    drain_token: str,
    *,
    draining: bool,
    timeout_seconds: float = 15,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        payload, _ = await gateway_json(
            session,
            gateway_url,
            token,
            "GET",
            "/v1/nodes",
        )
        nodes = payload.get("nodes")
        if isinstance(nodes, list):
            node = next(
                (
                    item
                    for item in nodes
                    if isinstance(item, dict) and item.get("node_id") == node_id
                ),
                None,
            )
            if node is not None and (
                (
                    draining
                    and node.get("draining") is True
                    and node.get("admission_open") is False
                    and node.get("drain_token") == drain_token
                )
                or (
                    not draining
                    and node.get("draining") is False
                    and node.get("admission_open") is True
                )
            ):
                return
        await asyncio.sleep(0.1)
    direction = "draining" if draining else "admission-open"
    raise TimeoutError(f"gateway did not observe node {node_id} as {direction}")


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
    if args.force_different_worker and not args.detach_before_wake:
        raise ValueError("--force-different-worker requires --detach-before-wake")
    if args.force_different_worker and args.node_control_token_file is None:
        raise ValueError("--force-different-worker requires --node-control-token-file")
    token = read_token(args.gateway_token_file)
    sandbox_api_token = (
        read_token(args.sandbox_api_token_file)
        if args.sandbox_api_token_file is not None
        else token
    )
    node_control_token = (
        read_token(args.node_control_token_file)
        if args.node_control_token_file is not None
        else ""
    )
    suffix = uuid4().hex[:10]
    trace_id = uuid4().hex
    trace_headers = {"traceparent": f"00-{trace_id}-{uuid4().hex[:16]}-01"}
    sandbox_id = f"park-bench-{suffix}"
    job_id = f"counter-{suffix}"
    cycles: list[dict[str, Any]] = []
    started = time.monotonic()

    async with AsyncSandboxClient(
        args.gateway_url,
        api_token=sandbox_api_token,
        timeout_seconds=30,
        headers=trace_headers,
    ) as client, aiohttp.ClientSession(headers=trace_headers) as session:
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
            exec_probe = await handle.exec(
                ["python", "-c", "print('sdk-exec-ok')"],
                timeout_seconds=30,
            )
            if not exec_probe.success or exec_probe.stdout.strip() != "sdk-exec-ok":
                raise RuntimeError(
                    "sandbox-scoped SDK exec failed: "
                    f"status={exec_probe.status!r} "
                    f"exit_code={exec_probe.exit_code!r} "
                    f"stdout={exec_probe.stdout!r} stderr={exec_probe.stderr!r}"
                )
            await handle.upload_file("/workspace/counter.py", COUNTER_PROGRAM)
            job_handle = await handle.start_job(
                ["python", "/workspace/counter.py"],
                job_id=job_id,
                working_dir="/workspace",
            )
            baseline_job = job_handle.record
            baseline_pid = baseline_job.pid
            baseline_spec = baseline_job.spec_sha256

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
                parked_job = await client.get_job(sandbox_id, job_id)

                parked_record = parked.get("sandbox", {})
                parked_route = await cached_sandbox_route(
                    session,
                    args.gateway_url,
                    token,
                    sandbox_id,
                )
                source_node = parked_route.get("node")
                source_node = source_node if isinstance(source_node, dict) else {}
                source_node_id = str(source_node.get("node_id") or "")
                source_node_url = str(source_node.get("node_url") or "")
                if parked_route.get("cached_state") != "parked":
                    raise RuntimeError(
                        "gateway did not persist the parked route: "
                        + json.dumps(parked_route, sort_keys=True)
                    )
                drain_token = f"detach-benchmark:{suffix}:{index}"
                drained = False
                detached: dict[str, Any] = {}
                detach_retries = 0
                detach_seconds = 0.0
                try:
                    if args.force_different_worker:
                        if not source_node_url:
                            raise RuntimeError("park response omitted source node URL")
                        await node_json(
                            session,
                            source_node_url,
                            node_control_token,
                            "POST",
                            "/v1/drain",
                            payload={"token": drain_token, "draining": True},
                        )
                        drained = True
                        await wait_for_gateway_drain_state(
                            session,
                            args.gateway_url,
                            token,
                            source_node_id,
                            drain_token,
                            draining=True,
                        )
                    if args.detach_before_wake:
                        detach_started = time.monotonic()
                        detached, detach_retries = await gateway_json(
                            session,
                            args.gateway_url,
                            token,
                            "POST",
                            f"/v1/sandboxes/{sandbox_id}/detach",
                            payload={},
                            attempts=101,
                        )
                        detach_seconds = time.monotonic() - detach_started

                    wake_started = time.monotonic()
                    if args.wake_via_exec:
                        woke, wake_retries = await gateway_json(
                            session,
                            args.gateway_url,
                            sandbox_api_token,
                            "POST",
                            f"/v1/sandboxes/{sandbox_id}/exec",
                            payload={
                                "command": ["true"],
                                "env": {},
                                "working_dir": None,
                                "stdin": False,
                                "tty": False,
                            },
                            attempts=101,
                        )
                    else:
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
                finally:
                    if drained:
                        await node_json(
                            session,
                            source_node_url,
                            node_control_token,
                            "POST",
                            "/v1/drain",
                            payload={"token": drain_token, "draining": False},
                        )
                        await wait_for_gateway_drain_state(
                            session,
                            args.gateway_url,
                            token,
                            source_node_id,
                            drain_token,
                            draining=False,
                        )
                next_counter = await read_counter(current_counter)
                running_job = await client.get_job(sandbox_id, job_id)
                running_route = await cached_sandbox_route(
                    session,
                    args.gateway_url,
                    token,
                    sandbox_id,
                )
                destination_node = running_route.get("node")
                destination_node = (
                    destination_node if isinstance(destination_node, dict) else {}
                )
                woke_record = woke.get("sandbox", {})
                if args.wake_via_exec:
                    woke_record = {"state": running_route.get("cached_state")}
                detached_record = detached.get("sandbox", {})
                cycles.append(
                    {
                        "cycle": index,
                        "park_seconds": park_seconds,
                        "wake_seconds": wake_seconds,
                        "detach_seconds": detach_seconds,
                        "park_retries": park_retries,
                        "wake_retries": wake_retries,
                        "wake_timings": woke.get("timings", {}),
                        "detach_retries": detach_retries,
                        "parked_state": parked_record.get("state"),
                        "woke_state": woke_record.get("state"),
                        "detached_worker_state": detached_record.get("worker_state"),
                        "snapshot_manifest_digest": detached_record.get(
                            "snapshot_manifest_digest"
                        ),
                        "source_node_id": source_node_id,
                        "destination_node_id": destination_node.get("node_id"),
                        "counter_before": current_counter,
                        "counter_after": next_counter,
                        "parked_pid": parked_job.pid,
                        "running_pid": running_job.pid,
                        "parked_spec_sha256": parked_job.spec_sha256,
                        "running_spec_sha256": running_job.spec_sha256,
                    }
                )
                current_counter = next_counter

            park_values = [item["park_seconds"] for item in cycles]
            wake_values = [item["wake_seconds"] for item in cycles]
            correctness = {
                "sandbox_scoped_sdk_exec_succeeded": exec_probe.success,
                "every_park_reached_parked": all(
                    item["parked_state"] == "parked" for item in cycles
                ),
                "every_wake_reached_running": all(
                    item["woke_state"] == "running" for item in cycles
                ),
                "counter_advanced_after_every_wake": all(
                    item["counter_after"] > item["counter_before"] for item in cycles
                ),
                "process_identity_preserved": all(
                    item["parked_pid"] == baseline_pid
                    and item["running_pid"] == baseline_pid
                    and item["parked_spec_sha256"] == baseline_spec
                    and item["running_spec_sha256"] == baseline_spec
                    for item in cycles
                ),
            }
            if args.detach_before_wake:
                correctness["every_detach_reached_detached"] = all(
                    item["detached_worker_state"] == "detached"
                    and bool(item["snapshot_manifest_digest"])
                    for item in cycles
                )
            if args.force_different_worker:
                correctness["every_wake_used_another_worker"] = all(
                    item["source_node_id"]
                    and item["destination_node_id"]
                    and item["source_node_id"] != item["destination_node_id"]
                    for item in cycles
                )
            detach_values = [item["detach_seconds"] for item in cycles]
            return {
                "ok": all(correctness.values()),
                "trace_id": trace_id,
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
                    "detach_min_seconds": min(detach_values),
                    "detach_median_seconds": statistics.median(detach_values),
                    "detach_max_seconds": max(detach_values),
                    "total_lifecycle_retries": sum(
                        item["park_retries"]
                        + item["detach_retries"]
                        + item["wake_retries"]
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
