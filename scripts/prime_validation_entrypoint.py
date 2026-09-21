"""Record resolved task resources and apply explicit qualification image aliases."""

import argparse
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


def resource_evidence(config, expected: dict[str, float]) -> dict:
    """Inspect task-resolved values, never guest env or credentials."""
    resolved = {key: getattr(config, key) for key in ("cpu", "memory", "disk")}
    mismatches = {
        key: {"expected": value, "resolved": resolved[key]}
        for key, value in expected.items()
        if resolved[key] != value
    }
    return {
        "evidence_level": "resolved-runtime-config",
        "resources": resolved,
        "resource_units": {"cpu": "cores", "memory": "GiB", "disk": "GiB"},
        "expected_resources": expected,
        "mismatches": mismatches,
    }


def parse_arguments(argv: list[str]):
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("aliases", help="Alias JSON file, or '-' for no aliases")
    parser.add_argument("taskset")
    parser.add_argument("--qualification-evidence", type=Path)
    for resource in ("cpu", "memory", "disk"):
        parser.add_argument(f"--qualification-{resource}", type=float)
    return parser.parse_known_args(argv)


def main() -> None:
    from verifiers_ucloud.runtime import UCloudRuntime

    args, remainder = parse_arguments(sys.argv[1:])
    sys.argv = [sys.argv[0], args.taskset, *remainder]
    rules = (
        []
        if args.aliases == "-"
        else json.loads(Path(args.aliases).read_text())["rules"]
    )
    expected = {
        resource: value
        for resource in ("cpu", "memory", "disk")
        if (value := getattr(args, f"qualification_{resource}")) is not None
    }
    original_start = UCloudRuntime.start

    async def start(self):
        original = self.config.image
        resolved = resolve_image(args.taskset, original, rules)
        record = {
            "schema_version": 1,
            "taskset": args.taskset,
            "runtime_name": self.name,
            "source_image": original,
            "resolved_image": resolved,
            **resource_evidence(self.config, expected),
        }
        if args.qualification_evidence is not None:
            with args.qualification_evidence.open("a") as stream:
                stream.write(json.dumps(record, sort_keys=True) + "\n")
        if record["mismatches"]:
            raise ValueError(
                "task-resolved resources differ from explicit qualification limits: "
                + json.dumps(record["mismatches"], sort_keys=True)
            )
        if resolved != original:
            print(f"Image alias: {original} -> {resolved}", flush=True)
            self.config.image = resolved
            self.info.image = resolved
        await original_start(self)

    UCloudRuntime.start = start
    runpy.run_module("verifiers.v1.cli.validate", run_name="__main__")


if __name__ == "__main__":
    main()
