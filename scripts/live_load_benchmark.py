#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys
from tempfile import TemporaryDirectory
import threading
import time
from typing import Any, Callable, Iterable, TypeVar


REPO_ROOT = Path(__file__).resolve().parents[1]
SDK_SRC = REPO_ROOT / "ucloud-sandboxes-sdk" / "src"
if SDK_SRC.is_dir():
    sys.path.insert(0, str(SDK_SRC))

from ucloud_sandboxes_sdk import Image, SandboxApiError, SandboxClient  # noqa: E402


T = TypeVar("T")
R = TypeVar("R")
PRINT_LOCK = threading.Lock()


def emit(event: str, **fields: Any) -> None:
    payload = {
        "event": event,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    with PRINT_LOCK:
        print(json.dumps(payload, sort_keys=True), flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build fresh images, ramp a live UCloud sandbox deployment to a target "
            "count, exercise concurrent exec, and clean up sandboxes/capacity signals."
        )
    )
    parser.add_argument("--gateway-url", default="http://127.0.0.1:8090")
    parser.add_argument(
        "--token-file",
        type=Path,
        default=Path("/work/ucloud-sandboxes/state/gateway-token"),
    )
    parser.add_argument("--run-id", default="")
    parser.add_argument(
        "--registry-prefix",
        default="ucloud-sandbox-registry:5000/benchmarks",
    )
    parser.add_argument("--sandboxes", type=int, default=100)
    parser.add_argument("--cpus", type=float, default=1.0)
    parser.add_argument("--memory-mb", type=int, default=512)
    parser.add_argument("--disk-mb", type=int, default=1024)
    parser.add_argument("--capacity-lead-seconds", type=float, default=15.0)
    parser.add_argument("--sandbox-ttl-seconds", type=int, default=1200)
    parser.add_argument("--prepare-ttl-seconds", type=int, default=1800)
    parser.add_argument("--build-timeout-seconds", type=float, default=1800.0)
    parser.add_argument("--create-timeout-seconds", type=float, default=1800.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--exec-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--max-create-workers", type=int, default=100)
    parser.add_argument("--max-exec-workers", type=int, default=100)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--keep-sandboxes", action="store_true")
    args = parser.parse_args()
    if args.sandboxes <= 0:
        parser.error("--sandboxes must be positive")
    if args.cpus <= 0 or args.memory_mb <= 0 or args.disk_mb <= 0:
        parser.error("sandbox resources must be positive")
    if args.max_create_workers <= 0 or args.max_exec_workers <= 0:
        parser.error("worker counts must be positive")
    return args


def main() -> int:
    args = parse_args()
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    token = args.token_file.read_text(encoding="utf-8").strip()
    if not token:
        raise SystemExit(f"empty gateway token file: {args.token_file}")
    client = SandboxClient(
        args.gateway_url,
        api_token=token,
        timeout_seconds=args.request_timeout_seconds,
    )
    health = client.health()
    emit("benchmark_start", run_id=run_id, sandboxes=args.sandboxes, health=health)

    summary: dict[str, Any] = {
        "run_id": run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "configuration": {
            "sandboxes": args.sandboxes,
            "cpus": args.cpus,
            "memory_mb": args.memory_mb,
            "disk_mb": args.disk_mb,
            "capacity_lead_seconds": args.capacity_lead_seconds,
            "sandbox_ttl_seconds": args.sandbox_ttl_seconds,
        },
        "health": health,
        "metrics": {},
        "builds": [],
        "capacity": [],
        "ramps": [],
        "exec": {},
        "cleanup": {},
        "errors": [],
    }
    handles: dict[str, Any] = {}
    capacity_ids: list[str] = []
    builder_prepare_id = f"bench-builder-{run_id}"
    benchmark_failed = False

    try:
        summary["metrics"]["baseline"] = metrics_snapshot(client)
        client.prepare_builder(
            prepare_id=builder_prepare_id,
            count=1,
            ttl_seconds=args.prepare_ttl_seconds,
        )
        emit("builder_capacity_requested", prepare_id=builder_prepare_id)

        with TemporaryDirectory(prefix=f"ucloud-bench-{run_id}-") as raw_context_root:
            images = make_build_images(
                Path(raw_context_root),
                run_id=run_id,
                registry_prefix=args.registry_prefix,
            )
            image_counts = split_counts(args.sandboxes, len(images))
            capacity_started = time.monotonic()
            for index, count in enumerate(image_counts):
                prepare_id = f"bench-capacity-{run_id}-{index}"
                capacity_ids.append(prepare_id)
                response = client.prepare_capacity(
                    prepare_id=prepare_id,
                    count=count,
                    cpus=args.cpus,
                    memory_mb=args.memory_mb,
                    disk_mb=args.disk_mb,
                    ttl_seconds=args.prepare_ttl_seconds,
                )
                summary["capacity"].append(
                    {
                        "prepare_id": prepare_id,
                        "count": count,
                        "image": "",
                        "boot_response": response,
                    }
                )
                emit(
                    "sandbox_capacity_requested",
                    prepare_id=prepare_id,
                    count=count,
                    image="",
                    phase="boot",
                )

            build_results = run_parallel(
                images,
                lambda image: build_one(
                    client,
                    image,
                    timeout_seconds=args.build_timeout_seconds,
                ),
                max_workers=len(images),
            )
            summary["builds"] = build_results
            failed_builds = [item for item in build_results if not item["ok"]]
            if failed_builds:
                raise RuntimeError(f"{len(failed_builds)} image build(s) failed")

            registry_images = [
                Image.from_registry(str(item["tag"])) for item in build_results
            ]
            summary["metrics"]["after_builds"] = metrics_snapshot(client)

            for index, (image, count) in enumerate(
                zip(registry_images, image_counts, strict=True)
            ):
                prepare_id = capacity_ids[index]
                response = client.prepare_capacity(
                    prepare_id=prepare_id,
                    count=count,
                    cpus=args.cpus,
                    memory_mb=args.memory_mb,
                    disk_mb=args.disk_mb,
                    image=image,
                    ttl_seconds=args.prepare_ttl_seconds,
                )
                summary["capacity"][index].update(
                    image=image.reference,
                    warmup_response=response,
                )
                emit(
                    "sandbox_image_warmup_requested",
                    prepare_id=prepare_id,
                    count=count,
                    image=image.reference,
                    phase="warmup",
                )

            wait_with_metrics(
                client,
                seconds=max(0.0, args.capacity_lead_seconds),
                label="capacity_lead",
            )
            summary["metrics"]["after_capacity_lead"] = metrics_snapshot(client)

            ramp_targets = ramp_targets_for(args.sandboxes)
            previous_target = 0
            for target in ramp_targets:
                indexes = list(range(previous_target, target))
                ramp_started = time.monotonic()
                results = run_parallel(
                    indexes,
                    lambda index: create_one(
                        client,
                        run_id=run_id,
                        index=index,
                        image=registry_images[index % len(registry_images)],
                        args=args,
                    ),
                    max_workers=min(args.max_create_workers, len(indexes)),
                )
                for item in results:
                    if item["ok"]:
                        handles[str(item["sandbox_id"])] = item.pop("handle")
                duration = time.monotonic() - ramp_started
                ramp = {
                    "from": previous_target,
                    "target": target,
                    "duration_seconds": round(duration, 3),
                    "latency_ms": latency_summary(
                        [float(item["duration_ms"]) for item in results]
                    ),
                    "succeeded": sum(bool(item["ok"]) for item in results),
                    "failed": sum(not bool(item["ok"]) for item in results),
                    "errors": [item.get("error") for item in results if not item["ok"]],
                    "capacity_request_age_seconds": round(
                        time.monotonic() - capacity_started,
                        3,
                    ),
                }
                summary["ramps"].append(ramp)
                emit("sandbox_ramp_complete", **ramp)
                summary["metrics"][f"after_{target}_sandboxes"] = metrics_snapshot(
                    client
                )
                if ramp["failed"]:
                    raise RuntimeError(
                        f"sandbox ramp to {target} had {ramp['failed']} failure(s)"
                    )
                previous_target = target

            handles_in_order = [handles[key] for key in sorted(handles)]
            summary["exec"]["light"] = run_exec_round(
                handles_in_order,
                command=["sh", "-lc", "printf ready"],
                timeout_seconds=args.exec_timeout_seconds,
                max_workers=args.max_exec_workers,
                label="light",
            )
            summary["metrics"]["after_light_exec"] = metrics_snapshot(client)
            summary["exec"]["cpu_io"] = run_exec_round(
                handles_in_order,
                command=[
                    "sh",
                    "-lc",
                    (
                        "set -e; dd if=/dev/zero bs=1M count=16 2>/dev/null "
                        "| sha256sum > /tmp/ucloud-bench.sha; "
                        "test -s /tmp/ucloud-bench.sha; "
                        "rm -f /tmp/ucloud-bench.sha; uname -s"
                    ),
                ],
                timeout_seconds=args.exec_timeout_seconds,
                max_workers=args.max_exec_workers,
                label="cpu_io",
            )
            summary["metrics"]["after_cpu_io_exec"] = metrics_snapshot(client)
    except BaseException as exc:
        benchmark_failed = True
        summary["errors"].append(f"{type(exc).__name__}: {exc}")
        emit("benchmark_error", error=summary["errors"][-1])
    finally:
        undeleted_handles = len(handles) if args.keep_sandboxes else 0
        if not args.keep_sandboxes:
            cleanup_results = run_parallel(
                sorted(handles),
                lambda sandbox_id: delete_one(client, sandbox_id),
                max_workers=min(50, max(1, len(handles))),
            )
            sandbox_cleanup = {
                "attempted": len(cleanup_results),
                "succeeded": sum(bool(item["ok"]) for item in cleanup_results),
                "failed": sum(not bool(item["ok"]) for item in cleanup_results),
                "errors": [
                    item.get("error") for item in cleanup_results if not item["ok"]
                ],
            }
            summary["cleanup"]["sandboxes"] = sandbox_cleanup
            undeleted_handles = int(sandbox_cleanup["failed"])
            if undeleted_handles:
                benchmark_failed = True
                summary["errors"].append(
                    f"cleanup: {undeleted_handles} sandbox delete(s) failed"
                )
        capacity_cleanup = [
            delete_prepare(client.delete_prepared_capacity, prepare_id)
            for prepare_id in capacity_ids
        ]
        summary["cleanup"]["capacity"] = summarize_cleanup(capacity_cleanup)
        builder_cleanup = delete_prepare(
            client.delete_prepared_builder,
            builder_prepare_id,
        )
        summary["cleanup"]["builders"] = summarize_cleanup([builder_cleanup])
        prepare_failures = sum(
            int(summary["cleanup"][kind]["failed"])
            for kind in ("capacity", "builders")
        )
        if prepare_failures:
            benchmark_failed = True
            summary["errors"].append(
                f"cleanup: {prepare_failures} capacity signal delete(s) failed"
            )
        try:
            summary["metrics"]["after_cleanup"] = metrics_snapshot(client)
        except Exception as exc:
            summary["errors"].append(f"cleanup metrics: {type(exc).__name__}: {exc}")
        summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        summary["ok"] = not benchmark_failed and not summary["errors"]
        emit(
            "benchmark_complete",
            run_id=run_id,
            ok=summary["ok"],
            undeleted_handles=undeleted_handles,
        )
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(
                json.dumps(summary, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        print("UCLOUD_BENCHMARK_RESULT=" + json.dumps(summary, sort_keys=True))
    return 0 if summary["ok"] else 1


def make_build_images(
    root: Path,
    *,
    run_id: str,
    registry_prefix: str,
) -> list[Image]:
    definitions = [
        (
            "python",
            """FROM python:3.12-slim
ARG BENCH_RUN_ID
RUN pip install --no-cache-dir requests==2.32.4 pydantic==2.11.7
COPY payload.bin /opt/ucloud-bench/payload.bin
RUN python -c \"import hashlib; print(hashlib.sha256(open('/opt/ucloud-bench/payload.bin','rb').read()).hexdigest())\" \\
 && printf '%s\\n' \"$BENCH_RUN_ID\" > /opt/ucloud-bench/run-id
CMD [\"sleep\", \"900\"]
""",
        ),
        (
            "node",
            """FROM node:22-bookworm-slim
ARG BENCH_RUN_ID
RUN npm install --global typescript@5.8.3
COPY payload.bin /opt/ucloud-bench/payload.bin
RUN sha256sum /opt/ucloud-bench/payload.bin \\
 && printf '%s\\n' \"$BENCH_RUN_ID\" > /opt/ucloud-bench/run-id
CMD [\"sleep\", \"900\"]
""",
        ),
        (
            "ubuntu",
            """FROM ubuntu:24.04
ARG BENCH_RUN_ID
RUN apt-get update \\
 && apt-get install -y --no-install-recommends ca-certificates curl jq python3 \\
 && rm -rf /var/lib/apt/lists/*
COPY payload.bin /opt/ucloud-bench/payload.bin
RUN sha256sum /opt/ucloud-bench/payload.bin \\
 && printf '%s\\n' \"$BENCH_RUN_ID\" > /opt/ucloud-bench/run-id
CMD [\"sleep\", \"900\"]
""",
        ),
    ]
    payload = deterministic_payload(2 * 1024 * 1024)
    images: list[Image] = []
    for slug, dockerfile in definitions:
        context = root / slug
        context.mkdir(parents=True)
        (context / "Dockerfile").write_text(dockerfile, encoding="utf-8")
        (context / "payload.bin").write_bytes(payload)
        tag = f"{registry_prefix.rstrip('/')}/{run_id}/{slug}:latest"
        images.append(
            Image.from_dockerfile(
                image_id=f"bench-{run_id}-{slug}",
                tag=tag,
                context_path=context,
                build_args={"BENCH_RUN_ID": run_id},
                labels={"ucloud-sandboxes.benchmark.run-id": run_id},
            )
        )
    return images


def deterministic_payload(size: int) -> bytes:
    output = bytearray()
    counter = 0
    while len(output) < size:
        output.extend(hashlib.sha256(f"ucloud-bench-{counter}".encode()).digest())
        counter += 1
    return bytes(output[:size])


def build_one(
    client: SandboxClient,
    image: Image,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    started = time.monotonic()
    tag = str(image.tag or image.reference)
    emit("image_build_started", image=image.reference, tag=tag)
    try:
        build = client.build_image(
            image,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=2.0,
            retry_interval_seconds=1.0,
            on_status=lambda raw: emit(
                "image_build_status",
                image=image.reference,
                build_id=raw.get("build_id"),
                status=raw.get("status"),
                updated_at=raw.get("updated_at"),
            ),
        )
        completed = build.get("build") if isinstance(build.get("build"), dict) else build
        result = {
            "ok": True,
            "image": image.reference,
            "tag": tag,
            "duration_seconds": round(time.monotonic() - started, 3),
            "build_id": completed.get("build_id"),
            "status": completed.get("status"),
            "timings": completed.get("timings"),
            "started_at": completed.get("started_at"),
            "completed_at": completed.get("completed_at"),
        }
    except BaseException as exc:
        result = {
            "ok": False,
            "image": image.reference,
            "tag": tag,
            "duration_seconds": round(time.monotonic() - started, 3),
            "error": f"{type(exc).__name__}: {exc}",
        }
    emit("image_build_complete", **result)
    return result


def create_one(
    client: SandboxClient,
    *,
    run_id: str,
    index: int,
    image: Image,
    args: argparse.Namespace,
) -> dict[str, Any]:
    sandbox_id = f"bench-{run_id}-{index:03d}"
    started = time.monotonic()
    try:
        handle = client.create_sandbox(
            id=sandbox_id,
            image=image,
            command=["sleep", str(args.sandbox_ttl_seconds)],
            cpus=args.cpus,
            memory_mb=args.memory_mb,
            disk_mb=args.disk_mb,
            ttl_seconds=args.sandbox_ttl_seconds,
            network="none",
            filesystem={
                "enforce_disk_quota": True,
                "workspace_path": "/workspace",
                "tmpfs_mb": 64,
                "run_tmpfs_mb": 16,
            },
            labels={
                "benchmark.run_id": run_id,
                "benchmark.index": str(index),
            },
            start_timeout_seconds=args.create_timeout_seconds,
            request_timeout_seconds=args.request_timeout_seconds,
            retry_interval_seconds=1.0,
        )
        response = handle.create_response
        return {
            "ok": True,
            "sandbox_id": sandbox_id,
            "duration_ms": round((time.monotonic() - started) * 1000, 3),
            "timings": response.get("timings"),
            "node_id": response.get("node_id") or response.get("nodeId"),
            "handle": handle,
        }
    except BaseException as exc:
        return {
            "ok": False,
            "sandbox_id": sandbox_id,
            "duration_ms": round((time.monotonic() - started) * 1000, 3),
            "error": f"{type(exc).__name__}: {exc}",
        }


def run_exec_round(
    handles: list[Any],
    *,
    command: list[str],
    timeout_seconds: float,
    max_workers: int,
    label: str,
) -> dict[str, Any]:
    emit("exec_round_started", label=label, count=len(handles))

    def run_one(handle: Any) -> dict[str, Any]:
        started = time.monotonic()
        exec_started: float | None = None
        try:
            exec_handle = handle.client.start_exec(handle.id, command)
            exec_started = time.monotonic()
            result = exec_handle.wait(timeout_seconds=timeout_seconds)
            completed = time.monotonic()
            return {
                "ok": result.success,
                "duration_ms": round((completed - started) * 1000, 3),
                "start_duration_ms": round((exec_started - started) * 1000, 3),
                "wait_duration_ms": round((completed - exec_started) * 1000, 3),
                "start_ok": True,
                "wait_ok": True,
                "status": result.status,
                "exit_code": result.exit_code,
                "error": "" if result.success else result.stderr[-500:],
            }
        except BaseException as exc:
            completed = time.monotonic()
            failed = {
                "ok": False,
                "duration_ms": round((completed - started) * 1000, 3),
                "error": f"{type(exc).__name__}: {exc}",
            }
            if exec_started is None:
                failed["start_duration_ms"] = failed["duration_ms"]
                failed["start_ok"] = False
            else:
                failed["start_duration_ms"] = round(
                    (exec_started - started) * 1000,
                    3,
                )
                failed["wait_duration_ms"] = round(
                    (completed - exec_started) * 1000,
                    3,
                )
                failed["start_ok"] = True
                failed["wait_ok"] = False
            return failed

    started = time.monotonic()
    results = run_parallel(
        handles,
        run_one,
        max_workers=min(max_workers, max(1, len(handles))),
    )
    payload = {
        "count": len(results),
        "duration_seconds": round(time.monotonic() - started, 3),
        "succeeded": sum(bool(item["ok"]) for item in results),
        "failed": sum(not bool(item["ok"]) for item in results),
        "latency_ms": latency_summary(
            [float(item["duration_ms"]) for item in results]
        ),
        "start_latency_ms": latency_summary(
            [
                float(item["start_duration_ms"])
                for item in results
                if "start_duration_ms" in item
            ]
        ),
        "wait_latency_ms": latency_summary(
            [
                float(item["wait_duration_ms"])
                for item in results
                if "wait_duration_ms" in item
            ]
        ),
        "start_succeeded": sum(bool(item.get("start_ok")) for item in results),
        "start_failed": sum(item.get("start_ok") is False for item in results),
        "wait_succeeded": sum(bool(item.get("wait_ok")) for item in results),
        "wait_failed": sum(item.get("wait_ok") is False for item in results),
        "errors": [item.get("error") for item in results if not item["ok"]][:20],
    }
    emit("exec_round_complete", label=label, **payload)
    return payload


def delete_one(client: SandboxClient, sandbox_id: str) -> dict[str, Any]:
    try:
        client.delete_sandbox(sandbox_id)
        return {"ok": True, "sandbox_id": sandbox_id}
    except SandboxApiError as exc:
        if exc.status_code == 404:
            return {"ok": True, "sandbox_id": sandbox_id}
        return {
            "ok": False,
            "sandbox_id": sandbox_id,
            "error": f"SandboxApiError({exc.status_code}): {exc}",
        }
    except BaseException as exc:
        return {
            "ok": False,
            "sandbox_id": sandbox_id,
            "error": f"{type(exc).__name__}: {exc}",
        }


def delete_prepare(
    operation: Callable[[str], Any],
    prepare_id: str,
) -> dict[str, Any]:
    try:
        operation(prepare_id)
        emit("prepare_deleted", prepare_id=prepare_id)
        return {"ok": True, "prepare_id": prepare_id}
    except SandboxApiError as exc:
        if exc.status_code == 404:
            return {"ok": True, "prepare_id": prepare_id}
        error = f"SandboxApiError({exc.status_code}): {exc}"
        emit("prepare_delete_failed", prepare_id=prepare_id, error=error)
        return {"ok": False, "prepare_id": prepare_id, "error": error}
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        emit("prepare_delete_failed", prepare_id=prepare_id, error=error)
        return {"ok": False, "prepare_id": prepare_id, "error": error}


def summarize_cleanup(results: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "attempted": len(results),
        "succeeded": sum(bool(item["ok"]) for item in results),
        "failed": sum(not bool(item["ok"]) for item in results),
        "errors": [item.get("error") for item in results if not item["ok"]],
    }


def metrics_snapshot(client: SandboxClient) -> dict[str, Any]:
    payload = client._request_json("GET", "/v1/metrics?full=true")
    nodes = payload.get("nodes") if isinstance(payload.get("nodes"), dict) else {}
    lifecycle = payload.get("vm_lifecycle")
    lifecycle_items = lifecycle.get("items") if isinstance(lifecycle, dict) else []
    return {
        "generated_at": payload.get("generated_at"),
        "nodes": {
            key: nodes.get(key)
            for key in ("fresh", "sandbox", "builder", "compatible", "incompatible")
        },
        "resources": payload.get("resources"),
        "sandboxes": payload.get("sandboxes"),
        "capacity": payload.get("capacity"),
        "images": payload.get("images"),
        "builders": payload.get("builders"),
        "scale_up": payload.get("scale_up"),
        "exec": payload.get("exec"),
        "vm_lifecycle": {
            "items": lifecycle_items[-20:] if isinstance(lifecycle_items, list) else []
        },
    }


def wait_with_metrics(client: SandboxClient, *, seconds: float, label: str) -> None:
    deadline = time.monotonic() + seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        snapshot = metrics_snapshot(client)
        emit(
            "metrics_sample",
            label=label,
            nodes=snapshot.get("nodes"),
            sandboxes=snapshot.get("sandboxes"),
            capacity=snapshot.get("capacity"),
            images=snapshot.get("images"),
            builders=snapshot.get("builders"),
        )
        time.sleep(min(5.0, remaining))


def run_parallel(
    items: Iterable[T],
    operation: Callable[[T], R],
    *,
    max_workers: int,
) -> list[R]:
    values = list(items)
    if not values:
        return []
    results: list[R] = []
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        futures = [executor.submit(operation, item) for item in values]
        for future in as_completed(futures):
            results.append(future.result())
    return results


def split_counts(total: int, parts: int) -> list[int]:
    quotient, remainder = divmod(total, parts)
    return [quotient + (1 if index < remainder else 0) for index in range(parts)]


def ramp_targets_for(total: int) -> list[int]:
    candidates = [min(10, total), min(50, total), total]
    return list(dict.fromkeys(value for value in candidates if value > 0))


def latency_summary(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {
            "samples": 0,
            "min": None,
            "mean": None,
            "p50": None,
            "p95": None,
            "p99": None,
            "max": None,
        }
    ordered = sorted(values)
    return {
        "samples": len(ordered),
        "min": round(ordered[0], 3),
        "mean": round(statistics.fmean(ordered), 3),
        "p50": round(percentile(ordered, 0.50), 3),
        "p95": round(percentile(ordered, 0.95), 3),
        "p99": round(percentile(ordered, 0.99), 3),
        "max": round(ordered[-1], 3),
    }


def percentile(ordered: list[float], quantile: float) -> float:
    index = max(0, math.ceil(len(ordered) * quantile) - 1)
    return ordered[index]


if __name__ == "__main__":
    raise SystemExit(main())
