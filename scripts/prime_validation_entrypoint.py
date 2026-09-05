"""Explicit image aliases for qualification without editing upstream tasksets."""

import json
from pathlib import Path
import runpy
import sys


def resolve_image(taskset: str, image: str, rules: list[dict]) -> str:
    matches = [
        rule
        for rule in rules
        if rule["taskset"] == taskset
        and (
            image == rule["source"]
            if "source" in rule
            else image.startswith(rule["source_prefix"])
        )
    ]
    if len(matches) > 1:
        raise ValueError("ambiguous image alias")
    if not matches:
        return image
    rule = matches[0]
    if "source" in rule:
        return rule["target"]
    suffix = image[len(rule["source_prefix"]) :]
    if (
        not suffix
        or suffix.startswith("/")
        or ".." in suffix
        or any(c.isspace() for c in suffix)
    ):
        raise ValueError("invalid image alias suffix")
    return rule["target_prefix"] + suffix


def main() -> None:
    from verifiers_ucloud.runtime import UCloudRuntime

    path = Path(sys.argv[1])
    sys.argv = [sys.argv[0], *sys.argv[2:]]
    taskset = sys.argv[1]
    rules = json.loads(path.read_text())["rules"]
    original_start = UCloudRuntime.start

    async def start(self):
        original = self.config.image
        resolved = resolve_image(taskset, original, rules)
        if resolved != original:
            print(f"Image alias: {original} -> {resolved}", flush=True)
            self.config.image = resolved
            self.info.image = resolved
        await original_start(self)

    UCloudRuntime.start = start
    runpy.run_module("verifiers.v1.cli.validate", run_name="__main__")


if __name__ == "__main__":
    main()
