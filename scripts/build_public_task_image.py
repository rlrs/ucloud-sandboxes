#!/usr/bin/env python3
"""Build a pinned public task Dockerfile through UCloud and emit an exact alias."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import uuid


MANIFEST = Path(__file__).resolve().parents[1] / "docs/prime-public-builds.json"


def image_alias(row: dict, result: dict) -> dict:
    image = result.get("image") or {}
    digest = image.get("manifest_digest", "")
    tag = image.get("tag", "")
    if (
        result.get("status") != "succeeded"
        or not image.get("available_to_sandboxes")
        or image.get("state") != "available"
        or not image.get("pushed")
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
        or not tag
    ):
        raise ValueError("build did not publish an available digest-pinned image")
    reference = tag.split("@", 1)[0]
    if reference.rfind(":") > reference.rfind("/"):
        reference = reference.rsplit(":", 1)[0]
    return {
        "taskset": row["taskset"],
        "source": row["source_image"],
        "target": reference + "@" + digest,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--taskset", required=True)
    parser.add_argument(
        "--source",
        type=Path,
        required=True,
        help="Clean checkout of the manifest's pinned public source",
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="New evidence directory"
    )
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    matches = [
        row
        for row in json.loads(MANIFEST.read_text())["builds"]
        if row["taskset"] == args.taskset
    ]
    if len(matches) != 1:
        parser.error("taskset must select exactly one public build recipe")
    row = matches[0]
    source = args.source.resolve()
    context = (source / row["context"]).resolve()
    context.relative_to(source)
    commit = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()
    dirty = subprocess.check_output(
        [
            "git",
            "-C",
            str(source),
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            row["context"],
        ],
        text=True,
    )
    if commit != row["commit"] or dirty:
        parser.error("source must match the pinned commit with a clean build context")
    if (
        hashlib.sha256((context / "Dockerfile").read_bytes()).hexdigest()
        != row["dockerfile_sha256"]
    ):
        parser.error("Dockerfile digest differs from the public build manifest")
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "source.json").write_text(json.dumps(row, indent=2) + "\n")
    if not args.execute:
        print("Public build source verified; use --execute to build on UCloud")
        return 0
    from ucloud_sandboxes_sdk import Image, SandboxClient

    client = SandboxClient.from_env(timeout_seconds=950)
    result = client.build_image(
        Image.from_dockerfile(
            name="compat-public-" + uuid.uuid4().hex[:16], context_path=context
        ),
        timeout_seconds=2400,
    )
    # Preserve build evidence even if success/availability validation fails.
    (args.output / "build.json").write_text(json.dumps(result, indent=2) + "\n")
    alias = image_alias(row, result)
    (args.output / "aliases.json").write_text(
        json.dumps({"rules": [alias]}, indent=2) + "\n"
    )
    print(
        "Published an exact image alias; task execution and grading still require qualification"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
