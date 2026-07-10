#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
SDK_SRC = REPO_ROOT / "ucloud-sandboxes-sdk" / "src"
if SDK_SRC.exists():
    sys.path.insert(0, str(SDK_SRC))
sys.path.insert(0, str(REPO_ROOT))

from ucloud_sandboxes.tmax import TMaxBuildContext, materialize_tmax_context
from ucloud_sandboxes_sdk import Image, SandboxApiError, SandboxClient


DATASET_ROWS_URL = "https://datasets-server.huggingface.co/rows"


def main() -> int:
    args = parse_args()
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    context_root = Path(args.context_root).expanduser() / run_id
    rows = fetch_candidate_rows(args)
    contexts = select_contexts(rows, args=args, context_root=context_root, run_id=run_id)
    if not contexts:
        raise SystemExit("no buildable TMax rows found in scanned range")

    print(
        json.dumps(
            {
                "run_id": run_id,
                "selected": [
                    {
                        "row_idx": context.row_idx,
                        "task_id": context.task_id,
                        "image_id": context.image_id,
                        "tag": context.tag,
                        "context_path": str(context.context_path),
                    }
                    for context in contexts
                ],
            },
            indent=2,
        )
    )
    if args.dry_run:
        return 0

    client = SandboxClient(
        args.gateway_url,
        api_token=args.token,
        timeout_seconds=args.request_timeout_seconds,
    )
    if args.prepare_builder:
        client.prepare_builder(
            prepare_id=f"tmax-build-{run_id}",
            count=args.builder_count,
            ttl_seconds=args.prepare_ttl_seconds,
        )
    if args.prepare_sandboxes:
        client.prepare_capacity(
            prepare_id=f"tmax-sandbox-{run_id}",
            count=len(contexts),
            cpus=args.cpus,
            memory_mb=args.memory_mb,
            disk_mb=args.disk_mb,
            ttl_seconds=args.prepare_ttl_seconds,
        )

    results: list[dict[str, Any]] = []
    for context in contexts:
        print(f"building {context.task_id} as {context.tag}", flush=True)
        build = build_with_retry(client, context, args=args)
        result: dict[str, Any] = {
            "task_id": context.task_id,
            "row_idx": context.row_idx,
            "image_id": context.image_id,
            "tag": context.tag,
            "build": build.get("build", build),
        }
        if not args.skip_sandbox:
            print(f"creating sandbox for {context.task_id}", flush=True)
            result["sandbox"] = run_sandbox_smoke(client, context, args=args, run_id=run_id)
        results.append(result)

    print(json.dumps({"run_id": run_id, "results": results}, indent=2, sort_keys=True))
    if not args.keep_contexts:
        remove_context_root(context_root)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build selected allenai/TMax-15K images through UCloud and smoke-test them as sandboxes.",
    )
    parser.add_argument("--gateway-url", default=os.getenv("UCLOUD_SANDBOX_API_URL", ""))
    parser.add_argument("--token", default=os.getenv("UCLOUD_SANDBOX_API_TOKEN", ""))
    parser.add_argument("--dataset", default="allenai/TMax-15K")
    parser.add_argument("--config", default="default")
    parser.add_argument("--split", default="train")
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--scan-limit", type=int, default=80)
    parser.add_argument("--page-size", type=int, default=20)
    parser.add_argument("--count", type=int, default=2)
    parser.add_argument("--context-root", default="/tmp/ucloud-tmax-build-contexts")
    parser.add_argument("--registry-prefix", default="ucloud-sandbox-registry:5000/tmax")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-sandbox", action="store_true")
    parser.add_argument("--keep-contexts", action="store_true")
    parser.add_argument("--keep-sandboxes", action="store_true")
    parser.add_argument("--prepare-builder", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--builder-count", type=int, default=1)
    parser.add_argument("--prepare-sandboxes", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--prepare-ttl-seconds", type=int, default=1800)
    parser.add_argument("--build-timeout-seconds", type=float, default=3600)
    parser.add_argument("--sandbox-timeout-seconds", type=float, default=1800)
    parser.add_argument("--request-timeout-seconds", type=float, default=120)
    parser.add_argument("--poll-interval-seconds", type=float, default=10)
    parser.add_argument("--retry-interval-seconds", type=float, default=15)
    parser.add_argument("--cpus", type=float, default=1.0)
    parser.add_argument("--memory-mb", type=int, default=2048)
    parser.add_argument("--disk-mb", type=int, default=10240)
    args = parser.parse_args()
    if not args.dry_run:
        if not args.gateway_url:
            parser.error("--gateway-url or UCLOUD_SANDBOX_API_URL is required")
        if not args.token:
            parser.error("--token or UCLOUD_SANDBOX_API_TOKEN is required")
    if args.count <= 0:
        parser.error("--count must be positive")
    if args.scan_limit <= 0:
        parser.error("--scan-limit must be positive")
    if args.page_size <= 0:
        parser.error("--page-size must be positive")
    return args


def fetch_candidate_rows(args: argparse.Namespace) -> list[tuple[int, dict[str, Any]]]:
    rows: list[tuple[int, dict[str, Any]]] = []
    offset = max(0, args.offset)
    end = offset + args.scan_limit
    while offset < end:
        length = min(args.page_size, end - offset)
        query = urlencode(
            {
                "dataset": args.dataset,
                "config": args.config,
                "split": args.split,
                "offset": offset,
                "length": length,
            }
        )
        with urlopen(f"{DATASET_ROWS_URL}?{query}", timeout=60) as response:
            payload = json.loads(response.read().decode("utf-8"))
        batch = payload.get("rows") if isinstance(payload, dict) else None
        if not isinstance(batch, list) or not batch:
            break
        for item in batch:
            if isinstance(item, dict) and isinstance(item.get("row"), dict):
                rows.append((int(item.get("row_idx", offset)), dict(item["row"])))
        offset += len(batch)
        if len(batch) < length:
            break
    return rows


def select_contexts(
    rows: list[tuple[int, dict[str, Any]]],
    *,
    args: argparse.Namespace,
    context_root: Path,
    run_id: str,
) -> list[TMaxBuildContext]:
    contexts: list[TMaxBuildContext] = []
    skipped: list[dict[str, Any]] = []
    for row_idx, row in rows:
        context = materialize_tmax_context(
            row,
            row_idx=row_idx,
            output_root=context_root,
            registry_prefix=args.registry_prefix,
            tag_suffix=run_id,
        )
        if context.buildable:
            contexts.append(context)
            if len(contexts) >= args.count:
                break
        else:
            skipped.append(
                {
                    "row_idx": row_idx,
                    "task_id": context.task_id,
                    "reason": context.skipped_reason,
                }
            )
    if skipped:
        print(json.dumps({"skipped": skipped[:20], "skipped_count": len(skipped)}, indent=2))
    return contexts


def build_with_retry(
    client: SandboxClient,
    context: TMaxBuildContext,
    *,
    args: argparse.Namespace,
) -> dict[str, Any]:
    image = Image.from_dockerfile(
        image_id=context.image_id,
        tag=context.tag,
        context_path=context.context_path,
        labels={"ucloud-sandboxes.tmax.task-id": context.task_id},
    )
    deadline = time.monotonic() + args.build_timeout_seconds
    submitted: dict[str, Any] | None = None
    while submitted is None:
        try:
            submitted = client.submit_image_build(
                image,
                timeout_seconds=args.request_timeout_seconds,
            )
        except SandboxApiError as exc:
            if exc.status_code != 503 or time.monotonic() >= deadline:
                raise
            print(f"builder not ready for {context.task_id}; retrying", flush=True)
            time.sleep(args.retry_interval_seconds)
    build_id = str(submitted.get("build_id") or submitted.get("image_id") or context.image_id)
    build = client.wait_for_image_build(
        build_id,
        timeout_seconds=max(1.0, deadline - time.monotonic()),
        poll_interval_seconds=args.poll_interval_seconds,
        on_status=lambda raw: print_build_status(context, raw),
    )
    if build.get("status") != "succeeded":
        raise SandboxApiError(
            f"TMax image build failed for {context.task_id}: {build.get('error') or build.get('status')}",
            body={"build": build},
        )
    return {"build": build}


def print_build_status(context: TMaxBuildContext, build: dict[str, Any]) -> None:
    tail = str(build.get("log_tail") or "")
    last_line = next((line for line in reversed(tail.splitlines()) if line.strip()), "")
    print(
        json.dumps(
            {
                "task_id": context.task_id,
                "build_id": build.get("build_id"),
                "status": build.get("status"),
                "updated_at": build.get("updated_at"),
                "last_log": last_line[-240:],
            },
            sort_keys=True,
        ),
        flush=True,
    )


def run_sandbox_smoke(
    client: SandboxClient,
    context: TMaxBuildContext,
    *,
    args: argparse.Namespace,
    run_id: str,
) -> dict[str, Any]:
    sandbox_id = f"{context.image_id}-{run_id}"[:80].rstrip(".-_")
    deadline = time.monotonic() + args.sandbox_timeout_seconds
    sandbox = None
    while sandbox is None:
        try:
            sandbox = client.create_sandbox(
                id=sandbox_id,
                image=Image.from_registry(context.tag),
                command=["sleep", str(int(args.sandbox_timeout_seconds))],
                cpus=args.cpus,
                memory_mb=args.memory_mb,
                disk_mb=args.disk_mb,
                ttl_seconds=int(args.sandbox_timeout_seconds),
                labels={"tmax.task_id": context.task_id, "tmax.run_id": run_id},
            )
        except SandboxApiError as exc:
            if exc.status_code not in (502, 503) or time.monotonic() >= deadline:
                raise
            print(
                f"sandbox capacity or image pull not ready for {context.task_id}; retrying",
                flush=True,
            )
            time.sleep(args.retry_interval_seconds)

    try:
        test_path = context.context_path / "test_initial_state.py"
        sandbox.upload_file_from_path(test_path, "/tmp/tmax_test_initial_state.py")
        result = sandbox.exec(
            ["python3", "-m", "pytest", "-q", "/tmp/tmax_test_initial_state.py"],
            timeout_seconds=max(1.0, deadline - time.monotonic()),
        )
        if not result.success:
            raise SandboxApiError(
                f"TMax sandbox initial-state test failed for {context.task_id}",
                body={
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "exit_code": result.exit_code,
                    "status": result.status,
                },
            )
        return {
            "sandbox_id": sandbox.id,
            "exit_code": result.exit_code,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    finally:
        if not args.keep_sandboxes:
            try:
                sandbox.delete()
            except Exception as exc:
                print(f"failed to delete sandbox {sandbox.id}: {exc}", file=sys.stderr)


def remove_context_root(context_root: Path) -> None:
    if not context_root.exists() or context_root == Path("/"):
        return
    for path in sorted(context_root.rglob("*"), reverse=True):
        if path.is_file() or path.is_symlink():
            path.unlink()
        elif path.is_dir():
            path.rmdir()
    context_root.rmdir()


if __name__ == "__main__":
    raise SystemExit(main())
