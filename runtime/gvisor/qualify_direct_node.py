#!/usr/bin/env python3
"""Exercise the product-facing direct node API on a real node."""

from __future__ import annotations

import argparse
import json
import time
from typing import Any
from urllib import error, parse, request


def _request(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    payload: object | None = None,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[object, float]:
    started = time.monotonic()
    merged_headers = dict(headers or {})
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        merged_headers["Content-Type"] = "application/json"
    req = request.Request(
        base_url.rstrip("/") + path,
        data=body,
        headers=merged_headers,
        method=method,
    )
    try:
        with request.urlopen(req, timeout=300) as response:
            raw = response.read()
            content_type = response.headers.get_content_type()
    except error.HTTPError as exc:
        raw = exc.read()
        try:
            detail = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            detail = raw.decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} returned {exc.code}: {detail}") from exc
    elapsed_ms = (time.monotonic() - started) * 1000
    if content_type == "application/json":
        return json.loads(raw), elapsed_ms
    return raw, elapsed_ms


def _sandbox(base_url: str, sandbox_id: str) -> dict[str, Any] | None:
    payload, _ = _request(base_url, "/v1/sandboxes")
    assert isinstance(payload, dict)
    for item in payload.get("sandboxes", []):
        if item.get("id") == sandbox_id:
            return item
    return None


def _exec(
    base_url: str,
    sandbox_id: str,
    command: list[str],
) -> tuple[dict[str, Any], float]:
    started = time.monotonic()
    payload, start_request_ms = _request(
        base_url,
        f"/v1/sandboxes/{parse.quote(sandbox_id, safe='')}/exec",
        method="POST",
        payload={"command": command},
    )
    assert isinstance(payload, dict)
    session = payload["session"]
    response_timings = payload.get("timings", {})
    server_start_ms = float(response_timings.get("start_ms", 0))
    manager_timings = response_timings.get("manager", {})
    session_id = parse.quote(session["id"], safe="")
    deadline = time.monotonic() + 300
    poll_count = 0
    while session["status"] not in {"exited", "failed"}:
        if time.monotonic() >= deadline:
            raise RuntimeError("exec session did not finish within 300 seconds")
        time.sleep(0.05)
        result, _ = _request(base_url, f"/v1/exec/{session_id}")
        poll_count += 1
        assert isinstance(result, dict)
        session = result["session"]
    completed_at = time.monotonic()
    events_payload, events_ms = _request(
        base_url, f"/v1/exec/{session_id}/events"
    )
    assert isinstance(events_payload, dict)
    events = events_payload["events"]
    stdout = "".join(
        str(item.get("data", "")) for item in events if item.get("stream") == "stdout"
    )
    stderr = "".join(
        str(item.get("data", "")) for item in events if item.get("stream") == "stderr"
    )
    if session.get("exit_code") != 0:
        raise RuntimeError(
            f"exec failed with {session.get('exit_code')}: {stderr.strip()}"
        )
    total_ms = (time.monotonic() - started) * 1000
    return {
        "session": session,
        "stdout": stdout,
        "stderr": stderr,
        "timings_ms": {
            "client_completion_wait": max(
                0.0,
                (completed_at - started) * 1000 - start_request_ms,
            ),
            "client_events": events_ms,
            "client_start_request": start_request_ms,
            "client_total": total_ms,
            "poll_count": poll_count,
            "server_start": server_start_ms,
            **{
                f"server_{key}": float(value)
                for key, value in manager_timings.items()
            },
        },
    }, total_ms


def _delete(base_url: str, sandbox: dict[str, Any]) -> float:
    _, elapsed_ms = _request(
        base_url,
        f"/v1/sandboxes/{parse.quote(sandbox['id'], safe='')}",
        method="DELETE",
        headers={
            "X-UCloud-Sandbox-Generation": str(sandbox["generation"]),
            "X-UCloud-Sandbox-Operation-Id": f"qualification-delete-{time.time_ns()}",
        },
    )
    return elapsed_ms


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8090")
    parser.add_argument(
        "--sandbox-id",
        help=(
            "Stable sandbox id. Defaults to a unique id; supply one explicitly "
            "for both phases of a restart test."
        ),
    )
    parser.add_argument("--image", default="busybox:latest")
    parser.add_argument(
        "--phase",
        choices=("full", "prepare-restart", "resume-after-restart"),
        default="full",
    )
    args = parser.parse_args()
    sandbox_id = args.sandbox_id or f"direct-qualification-{time.time_ns()}"

    timings: dict[str, float] = {}
    health, timings["health_ms"] = _request(args.base_url, "/healthz")
    heartbeat, timings["heartbeat_ms"] = _request(
        args.base_url, "/v1/heartbeat"
    )
    assert isinstance(heartbeat, dict)
    capabilities = heartbeat["heartbeat"]["capabilities"]
    required = {"sandbox", "hibernate-local-v1", "direct-runsc-v1"}
    if not required.issubset(capabilities):
        raise RuntimeError(f"direct node capabilities are incomplete: {capabilities}")

    existing = _sandbox(args.base_url, sandbox_id)
    if args.phase == "resume-after-restart":
        if existing is None or existing["state"] != "parked":
            raise RuntimeError("expected one durable parked sandbox after restart")
        executed, timings["wake_exec_ms"] = _exec(
            args.base_url,
            sandbox_id,
            ["/bin/sh", "-c", "cat /workspace/qualification; echo wake-ok"],
        )
        if executed["stdout"] != "persistent-payload\nwake-ok\n":
            raise RuntimeError(f"restored output mismatch: {executed['stdout']!r}")
        current = _sandbox(args.base_url, sandbox_id)
        assert current is not None
        timings["delete_ms"] = _delete(args.base_url, current)
        if _sandbox(args.base_url, sandbox_id) is not None:
            raise RuntimeError("sandbox remained after delete")
    else:
        if existing is not None:
            _delete(args.base_url, existing)
        _, timings["image_pull_ms"] = _request(
            args.base_url,
            "/v1/images/pull",
            method="POST",
            payload={"image": args.image, "id": "direct-qualification-image"},
        )
        created, timings["create_ms"] = _request(
            args.base_url,
            "/v1/sandboxes",
            method="POST",
            payload={
                "id": sandbox_id,
                "image": args.image,
                "command": ["/bin/sleep", "86400"],
                "cpus": 0.25,
                "memory_mb": 256,
                "disk_mb": 512,
                "parkable": True,
                "security": {"user": "0:0", "init": False},
            },
        )
        assert isinstance(created, dict)
        created_sandbox = created["sandbox"]
        if created_sandbox["state"] != "running":
            raise RuntimeError(f"create settled in {created_sandbox['state']}")
        payload = b"persistent-payload\n"
        _, timings["write_ms"] = _request(
            args.base_url,
            (
                f"/v1/sandboxes/{parse.quote(sandbox_id, safe='')}/files?"
                + parse.urlencode({"path": "/workspace/qualification"})
            ),
            method="PUT",
            body=payload,
            headers={"Content-Type": "application/octet-stream"},
        )
        downloaded, timings["read_ms"] = _request(
            args.base_url,
            (
                f"/v1/sandboxes/{parse.quote(sandbox_id, safe='')}/files?"
                + parse.urlencode({"path": "/workspace/qualification"})
            ),
        )
        if downloaded != payload:
            raise RuntimeError("binary file round-trip mismatch")
        executed, timings["exec_ms"] = _exec(
            args.base_url,
            sandbox_id,
            ["/bin/sh", "-c", "cat /workspace/qualification; echo live-ok"],
        )
        if executed["stdout"] != "persistent-payload\nlive-ok\n":
            raise RuntimeError(f"live output mismatch: {executed['stdout']!r}")
        parked, timings["park_ms"] = _request(
            args.base_url,
            f"/v1/sandboxes/{parse.quote(sandbox_id, safe='')}/park",
            method="POST",
            payload={"operation_id": f"qualification-park-{time.time_ns()}"},
        )
        assert isinstance(parked, dict)
        if parked["sandbox"]["state"] != "parked":
            raise RuntimeError("park did not release the running backend")
        if args.phase == "full":
            executed, timings["wake_exec_ms"] = _exec(
                args.base_url,
                sandbox_id,
                ["/bin/sh", "-c", "cat /workspace/qualification; echo wake-ok"],
            )
            if executed["stdout"] != "persistent-payload\nwake-ok\n":
                raise RuntimeError(f"restored output mismatch: {executed['stdout']!r}")
            current = _sandbox(args.base_url, sandbox_id)
            assert current is not None
            timings["delete_ms"] = _delete(args.base_url, current)

    result = {
        "health": health,
        "phase": args.phase,
        "sandbox_id": sandbox_id,
        "timings_ms": {key: round(value, 3) for key, value in timings.items()},
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
