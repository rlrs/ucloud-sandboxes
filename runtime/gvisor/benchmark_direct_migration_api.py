#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
from typing import Any
from urllib import error, parse, request


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark a real parked direct-runtime node-to-node migration."
    )
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--destination-url", required=True)
    parser.add_argument("--sandbox-id", required=True)
    parser.add_argument("--migration-id", required=True)
    parser.add_argument("--node-control-token-file", type=Path, required=True)
    return parser.parse_args()


def post(
    base_url: str,
    path: str,
    payload: dict[str, Any],
    *,
    token: str,
    timeout: float = 3600,
) -> tuple[dict[str, Any], float]:
    started = time.monotonic()
    outbound = request.Request(
        base_url.rstrip("/") + path,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with request.urlopen(outbound, timeout=timeout) as response:
            body = response.read()
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"POST {path} returned {exc.code}: {detail}") from exc
    return json.loads(body), (time.monotonic() - started) * 1000


def exec_command(
    base_url: str,
    sandbox_id: str,
    command: list[str],
    *,
    token: str,
) -> tuple[dict[str, Any], float]:
    started = time.monotonic()
    encoded = parse.quote(sandbox_id, safe="")
    response, start_ms = post(
        base_url,
        f"/v1/sandboxes/{encoded}/exec",
        {"command": command},
        token=token,
    )
    session = response["session"]
    session_id = parse.quote(str(session["id"]), safe="")
    headers = {"Authorization": f"Bearer {token}"}
    while session["status"] not in {"exited", "failed"}:
        time.sleep(0.05)
        outbound = request.Request(
            base_url.rstrip("/") + f"/v1/exec/{session_id}",
            headers=headers,
        )
        with request.urlopen(outbound, timeout=300) as polled:
            session = json.load(polled)["session"]
    outbound = request.Request(
        base_url.rstrip("/") + f"/v1/exec/{session_id}/events",
        headers=headers,
    )
    with request.urlopen(outbound, timeout=300) as events_response:
        events = json.load(events_response)["events"]
    stdout = "".join(
        str(item.get("data") or "")
        for item in events
        if item.get("stream") == "stdout"
    )
    stderr = "".join(
        str(item.get("data") or "")
        for item in events
        if item.get("stream") == "stderr"
    )
    if session.get("exit_code") != 0:
        raise RuntimeError(f"destination exec failed: {stderr.strip()}")
    return {
        "start_ms": round(start_ms, 3),
        "stdout": stdout,
    }, (time.monotonic() - started) * 1000


def main() -> int:
    args = arguments()
    token = args.node_control_token_file.read_text(encoding="utf-8").strip()
    if not token:
        raise SystemExit("node control token is empty")
    encoded = parse.quote(args.sandbox_id, safe="")
    prepared, prepare_ms = post(
        args.source_url,
        f"/v1/sandboxes/{encoded}/migration/prepare",
        {"migration_id": args.migration_id},
        token=token,
    )
    migration = prepared["migration"]
    archive_sha256 = str(migration["archive_sha256"])
    source_archive_url = (
        args.source_url.rstrip("/")
        + f"/v1/sandboxes/{encoded}/migration/archive"
        + "?migration_id="
        + parse.quote(args.migration_id, safe="")
    )
    activated = False
    try:
        _imported, import_ms = post(
            args.destination_url,
            "/v1/migrations/import",
            {
                "archive_sha256": archive_sha256,
                "archive_token": migration["archive_token"],
                "migration_id": args.migration_id,
                "sandbox_id": args.sandbox_id,
                "source_url": source_archive_url,
            },
            token=token,
        )
        _activated, activate_ms = post(
            args.destination_url,
            f"/v1/sandboxes/{encoded}/migration/activate",
            {
                "archive_sha256": archive_sha256,
                "migration_id": args.migration_id,
            },
            token=token,
        )
        activated = True
        exec_result, wake_exec_ms = exec_command(
            args.destination_url,
            args.sandbox_id,
            ["/bin/sh", "-c", "grep '^VmRSS:' /proc/1/status || true"],
            token=token,
        )
        _finalized, finalize_ms = post(
            args.source_url,
            f"/v1/sandboxes/{encoded}/migration/finalize",
            {
                "archive_sha256": archive_sha256,
                "migration_id": args.migration_id,
            },
            token=token,
        )
    except BaseException:
        if not activated:
            abort_payload = {
                "archive_sha256": archive_sha256,
                "migration_id": args.migration_id,
            }
            try:
                post(
                    args.destination_url,
                    f"/v1/sandboxes/{encoded}/migration/abort-import",
                    abort_payload,
                    token=token,
                )
            except BaseException:
                pass
            try:
                post(
                    args.source_url,
                    f"/v1/sandboxes/{encoded}/migration/abort",
                    abort_payload,
                    token=token,
                )
            except BaseException:
                pass
        raise
    result = {
        "archive_bytes": int(migration["archive_bytes"]),
        "sandbox_id": args.sandbox_id,
        "timings_ms": {
            "activate": round(activate_ms, 3),
            "finalize_source": round(finalize_ms, 3),
            "import_network_and_stage": round(import_ms, 3),
            "prepare_export": round(prepare_ms, 3),
            "wake_exec": round(wake_exec_ms, 3),
        },
        "wake_exec": exec_result,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
