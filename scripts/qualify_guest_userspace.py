#!/usr/bin/env python3
"""Exercise actual bootstrap/file scripts in disposable Linux containers.

This checks userspace portability, not gVisor, hibernation or taskset grading.
"""

import argparse
import json
from pathlib import Path
import shlex
import subprocess

from ucloud_sandboxes.direct_service import sandbox_file_write_script
from ucloud_sandboxes.sandbox import linux_host_entrypoint_script


def probe() -> str:
    upload = shlex.quote(sandbox_file_write_script())
    bootstrap = shlex.quote(linux_host_entrypoint_script())
    return f"""set -eu
printf 'root-file' | /bin/sh -c {upload} upload /eval.sh
[ "$(cat /eval.sh)" = root-file ]
printf 'unicode-file' | /bin/sh -c {upload} upload '/testbed/a space ü:file'
[ "$(cat '/testbed/a space ü:file')" = unicode-file ]
if printf bad | /bin/sh -c {upload} upload /testbed; then exit 1; fi
[ ! -e /testbed/.ucloud-write ]
UCLOUD_SANDBOX_KEEP_ALIVE=0 /bin/sh -c {bootstrap}
value=$(PATH=/opt/conda/bin:/bin /bin/sh -c {bootstrap} init /bin/sh -c 'printf "%s" "$PATH"')
[ "$value" = /opt/conda/bin:/bin ]
if PATH=/nonexistent UCLOUD_SANDBOX_ENABLE_CRON=1 /bin/sh -c {bootstrap}; then exit 1; fi
printf 'userspace checks passed\\n'
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ssh-host", help="Run Docker on this SSH host; otherwise use local Docker"
    )
    parser.add_argument("--image", action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    results = []
    for image in args.image:
        argv = [
            "docker",
            "run",
            "--rm",
            "-i",
            "--network=none",
            "--memory=128m",
            "--cpus=1",
            "--pids-limit=64",
            image,
            "/bin/sh",
        ]
        if args.ssh_host:
            argv = [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=10",
                args.ssh_host,
                shlex.join(argv),
            ]
        result = subprocess.run(
            argv, input=probe(), text=True, capture_output=True, timeout=180
        )
        results.append(
            {
                "image": image,
                "exit_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        )
    args.output.write_text(
        json.dumps(
            {
                "scope": "Linux userspace scripts only; not gVisor or taskset qualification",
                "results": results,
            },
            indent=2,
        )
        + "\n"
    )
    return int(any(row["exit_code"] for row in results))


if __name__ == "__main__":
    raise SystemExit(main())
