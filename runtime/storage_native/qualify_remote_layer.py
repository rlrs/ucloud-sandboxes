#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT))

from ucloud_sandboxes.storage_native import AgentEnvUblkClient  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend-socket", required=True, type=Path)
    parser.add_argument("--global-config", required=True, type=Path)
    parser.add_argument("--repo-blob-url", required=True)
    parser.add_argument("--digest", required=True)
    parser.add_argument("--size", required=True, type=int)
    parser.add_argument("--virtual-size", required=True, type=int)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def run(*argv: str) -> None:
    subprocess.run(argv, check=True)


def main() -> int:
    args = parse_args()
    client = AgentEnvUblkClient(args.backend_socket.resolve())
    with tempfile.TemporaryDirectory(
        prefix="remote-layer-",
        dir=args.work_root.resolve(),
    ) as raw_dir:
        root = Path(raw_dir)
        source = root / "source.json"
        runtime = root / "runtime"
        mount = root / "mount"
        mount.mkdir()
        source.write_text(
            json.dumps(
                {
                    "repoBlobUrl": args.repo_blob_url,
                    "lowers": [
                        {
                            "digest": args.digest,
                            "size": args.size,
                        }
                    ],
                    "resultFile": "",
                    "upper": {},
                },
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        started = time.monotonic()
        device = client.create_runtime_device(
            source_image_config=source.resolve(),
            global_config=args.global_config.resolve(),
            runtime_dir=runtime.resolve(),
            virtual_size=args.virtual_size,
            upper_mode="hybridLogStructured",
        )
        create_seconds = time.monotonic() - started
        mounted = False
        try:
            started = time.monotonic()
            run("mount", "-o", "noatime", str(device.device_path), str(mount))
            mounted = True
            mount_seconds = time.monotonic() - started
            top_level = sorted(path.name for path in mount.iterdir())
            files = sorted(
                str(path.relative_to(mount))
                for path in mount.rglob("*")
                if path.is_file()
            )
            if not top_level or not files:
                raise RuntimeError("remote snapshot mounted without expected state")
        finally:
            if mounted:
                run("umount", str(mount))
            client.delete(device.device_id)

    payload = {
        "create_seconds": create_seconds,
        "digest": args.digest,
        "file_count": len(files),
        "mount_seconds": mount_seconds,
        "size": args.size,
        "top_level": top_level,
    }
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
