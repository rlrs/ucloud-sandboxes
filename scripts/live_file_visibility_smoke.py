#!/usr/bin/env python3
"""Exercise production sandbox file visibility from upload through exec."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from uuid import uuid4


REPO_ROOT = Path(__file__).resolve().parents[1]
SDK_SRC = REPO_ROOT / "ucloud-sandboxes-sdk" / "src"
if SDK_SRC.is_dir():
    sys.path.insert(0, str(SDK_SRC))

from ucloud_sandboxes_sdk import (  # noqa: E402
    Image,
    SandboxClient,
    SandboxSpec,
)


SCRIPT_PATH = "/workspace/ucloud-pep723-smoke.py"
SCRIPT_BODY = b"""# /// script
# requires-python = \">=3.10\"
# dependencies = []
# ///
from pathlib import Path
import hashlib

raw = Path(__file__).read_bytes()
assert b\"# /// script\\n\" in raw
assert b\"# dependencies = []\\n\" in raw
print(\"UCLOUD_PEP723_OK:\" + hashlib.sha256(raw).hexdigest())
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create one production sandbox, upload a PEP 723 script, verify an "
            "exact read-back, execute it, and always request sandbox deletion."
        )
    )
    parser.add_argument(
        "--gateway-url",
        default="https://app-sandboxes.cloud.sdu.dk",
    )
    parser.add_argument("--token-file", type=Path)
    parser.add_argument(
        "--token-env",
        default="UCLOUD_SANDBOX_API_TOKEN",
    )
    parser.add_argument(
        "--image",
        default="ghcr.io/astral-sh/uv:python3.12-bookworm-slim",
    )
    parser.add_argument("--sandbox-id", default="")
    parser.add_argument("--create-timeout-seconds", type=float, default=600.0)
    parser.add_argument("--exec-timeout-seconds", type=float, default=120.0)
    return parser.parse_args()


def read_token(args: argparse.Namespace) -> str:
    token = os.environ.get(args.token_env, "").strip()
    if not token and args.token_file is not None:
        token = args.token_file.read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError(
            f"provide the sandbox API token through {args.token_env} or --token-file"
        )
    return token


def main() -> int:
    args = parse_args()
    sandbox_id = args.sandbox_id or (
        "prod-file-smoke-"
        + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        + "-"
        + uuid4().hex[:8]
    )
    expected_digest = hashlib.sha256(SCRIPT_BODY).hexdigest()
    client = SandboxClient(
        args.gateway_url,
        api_token=read_token(args),
        timeout_seconds=120.0,
    )
    result: dict[str, object] = {
        "sandbox_id": sandbox_id,
        "gateway_url": args.gateway_url,
        "image": args.image,
        "script_path": SCRIPT_PATH,
        "expected_size": len(SCRIPT_BODY),
        "expected_sha256": expected_digest,
    }
    cleanup_error = ""
    try:
        result["health"] = client.health()
        handle = client.create_sandbox(
            SandboxSpec(
                id=sandbox_id,
                image=Image.from_registry(args.image),
                command=("sleep", "900"),
                cpus=1.0,
                memory_mb=1024,
                disk_mb=4096,
                ttl_seconds=900,
                labels={"ucloud-sandboxes.smoke": "file-visibility"},
            ),
            request_timeout_seconds=args.create_timeout_seconds,
        )
        result["create"] = handle.create_response
        result["upload"] = handle.upload_file(SCRIPT_PATH, SCRIPT_BODY)
        downloaded = handle.download_file(SCRIPT_PATH)
        result["download_size"] = len(downloaded)
        result["download_sha256"] = hashlib.sha256(downloaded).hexdigest()
        if downloaded != SCRIPT_BODY:
            raise RuntimeError("downloaded script does not match the uploaded bytes")
        execution = handle.exec(
            ("python3", SCRIPT_PATH),
            timeout_seconds=args.exec_timeout_seconds,
        )
        result["exec"] = {
            "session_id": execution.session_id,
            "status": execution.status,
            "exit_code": execution.exit_code,
            "stdout": execution.stdout,
            "stderr": execution.stderr,
        }
        expected_stdout = f"UCLOUD_PEP723_OK:{expected_digest}"
        if not execution.success or execution.stdout.strip() != expected_stdout:
            raise RuntimeError("uploaded PEP 723 script did not execute successfully")
        result["ok"] = True
    except BaseException as exc:
        result["ok"] = False
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            result["delete"] = client.delete_sandbox(sandbox_id)
        except BaseException as exc:
            cleanup_error = f"{type(exc).__name__}: {exc}"
            result["cleanup_error"] = cleanup_error
            result["ok"] = False
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("ok") is True and not cleanup_error else 1


if __name__ == "__main__":
    raise SystemExit(main())
