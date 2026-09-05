#!/usr/bin/env python3
"""Exercise guest contracts on a live gateway; requires ucloud-sandboxes-sdk."""

import argparse
import json
import os
from pathlib import Path
import urllib.request
import uuid

from ucloud_sandboxes.sandbox import SandboxSpec


def main() -> int:
    from ucloud_sandboxes_sdk import SandboxClient

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists; choose a new evidence file")
    base = os.environ["UCLOUD_SANDBOX_URL"].rstrip("/")
    token = os.environ["UCLOUD_SANDBOX_API_TOKEN"]
    client = SandboxClient(base, api_token=token, timeout_seconds=900)
    cases = [
        ("ubuntu:noble-20260324", "linux_host", None),
        ("ubuntu:noble-20260324", "linux_session", None),
        ("alpine:3.20", "linux_session", None),
        ("ubuntu:noble-20260324", "linux_session", "nobody"),
    ]
    results = []
    for image, profile, user in cases:
        sandbox_id = "compat-" + uuid.uuid4().hex[:16]
        row = {"image": image, "profile": profile, "user": user}
        try:
            raw = {
                "id": sandbox_id,
                "image": image,
                "profile": profile,
                "memory_mb": 512,
                "disk_mb": 2048,
                "cpus": 1,
                "ttl_seconds": 1800,
                "env": {"COMPAT_LITERAL": "space ü : , $", "PATH": "/usr/bin:/bin"},
            }
            if user:
                raw["security"] = {"user": user}
            if image.startswith("alpine:"):
                raw["working_dir"] = "//workspace//nested/."
                row["working_dir"] = raw["working_dir"]
            spec = SandboxSpec.from_dict(raw)
            request = urllib.request.Request(
                base + "/v1/sandboxes",
                data=json.dumps(spec.to_dict()).encode(),
                headers={
                    "Authorization": "Bearer " + token,
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=900) as response:
                json.load(response)
            if "working_dir" in raw:
                startup = client.exec(
                    sandbox_id,
                    [
                        "/bin/sh",
                        "-c",
                        'test "$(readlink /proc/1/cwd)" = /workspace/nested',
                    ],
                    working_dir="/",
                    timeout_seconds=60,
                )
                if startup.exit_code != 0:
                    raise RuntimeError("startup cwd did not resolve inside the guest")
            target = "/eval.sh" if profile == "linux_host" else "/workspace/test ü.sh"
            client.upload_file(sandbox_id, target, "printf verified")
            result = client.exec(
                sandbox_id,
                [
                    "/bin/sh",
                    "-c",
                    'set -eu; test "$PATH" = /usr/bin:/bin; '
                    'test "$COMPAT_LITERAL" = "space ü : , $"; '
                    'test -r /proc/self/status; test "$(pwd)" = /; '
                    'uname -s; id; printf "HOME=%s\\n" "$HOME"; /bin/sh "$1"',
                    "probe",
                    target,
                ],
                working_dir="/",
                timeout_seconds=60,
            )
            row.update(
                exit_code=result.exit_code, stdout=result.stdout, stderr=result.stderr
            )
            if result.exit_code != 0 or "verified" not in result.stdout:
                raise RuntimeError("guest contract probe failed")
            row["status"] = "passed"
        except Exception as exc:
            # Avoid exception URLs: an endpoint can contain access credentials.
            row.update(status="failed", error_type=type(exc).__name__)
        finally:
            try:
                client.delete_sandbox(sandbox_id)
                row["cleanup"] = "deleted"
            except Exception as exc:
                row.update(cleanup="failed", cleanup_error_type=type(exc).__name__)
        results.append(row)
        args.output.write_text(json.dumps(results, indent=2) + "\n")
        print(image, profile, user, row["status"], row["cleanup"], flush=True)
    return int(
        any(row["status"] != "passed" or row["cleanup"] != "deleted" for row in results)
    )


if __name__ == "__main__":
    raise SystemExit(main())
