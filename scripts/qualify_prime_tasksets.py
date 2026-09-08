#!/usr/bin/env python3
"""Plan or run pinned upstream taskset checks; never infer success from CLI exit."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys


from ucloud_sandboxes.environment_contract import describe_environment
from ucloud_sandboxes.sandbox import SandboxSpec


MANIFEST = Path(__file__).resolve().parents[1] / "docs/prime-tasksets.json"


def verify_sources(source: Path, manifest: dict) -> None:
    commit = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()
    if commit != manifest["source_commit"]:
        raise ValueError(
            "research-environments checkout does not match the pinned commit"
        )
    for taskset in manifest["tasksets"]:
        for relative, expected in taskset["source_sha256"].items():
            path = source / relative
            if (
                path.is_symlink()
                or hashlib.sha256(path.read_bytes()).hexdigest() != expected
            ):
                raise ValueError(
                    f"taskset source differs from qualification manifest: {relative}"
                )


def verify_installed_sources(python: str, source: Path, rows: list[dict]) -> None:
    """An audited checkout is insufficient if Python imports a different copy."""
    modules = [row["taskset"].replace("-", "_") for row in rows]
    probe = (
        "import importlib.util,json,sys; "
        "print(json.dumps({name: (spec.origin if (spec := importlib.util.find_spec(name)) else None) "
        "for name in sys.argv[1:]}))"
    )
    locations = json.loads(
        subprocess.check_output([python, "-c", probe, *modules], text=True)
    )
    for row, module in zip(rows, modules):
        expected = source / row["package_path"] / module / "__init__.py"
        actual = locations.get(module)
        if actual is None or Path(actual).resolve() != expected.resolve():
            raise ValueError(
                f"install {row['package_path']} editable from the pinned checkout before execution"
            )


def _all_valid_counts(counts: object, total: int) -> bool:
    return (
        isinstance(counts, dict)
        and type(counts.get("valid")) is int
        and counts["valid"] == total
        and all(type(value) is int and value >= 0 for value in counts.values())
        and sum(counts.values()) == total
    )


def verdict(summary: object, *, mode: str, expected_total: int | None = None) -> str:
    """Reject malformed, contradictory, or shorter-than-requested evidence."""
    if not isinstance(summary, dict) or mode not in {"all", "setup"}:
        return "failed_or_incomplete"
    total = summary.get("total", 0)
    if (
        type(total) is not int
        or total < 1
        or type(summary.get("recorded")) is not int
        or summary.get("recorded") != total
        or summary.get("mode") != mode
        or not _all_valid_counts(summary.get("outcomes"), total)
        or (
            expected_total is not None
            and (type(expected_total) is not int or total != expected_total)
        )
    ):
        return "failed_or_incomplete"
    for field, expected in (("terminal", total), ("owed", 0)):
        if field in summary and (
            type(summary[field]) is not int or summary[field] != expected
        ):
            return "failed_or_incomplete"
    if mode == "all" or "checks" in summary:
        checks = summary.get("checks")
        if not isinstance(checks, dict):
            return "failed_or_incomplete"
        for check in ("gold", "setup") if mode == "all" else ("setup",):
            if not _all_valid_counts(checks.get(check), total):
                return "failed_or_incomplete"
    return "setup_passed" if mode == "setup" else "sample_passed"


def runtime_preflight(row: dict) -> dict:
    # Manifest requirements are pinned with the audited taskset sources. This
    # runs before importing a taskset, downloading datasets or building indices.
    spec = SandboxSpec(
        id="qualification-preflight",
        image="unresolved",
        memory_mb=128,
        required_features=tuple(row.get("required_runtime_features", ())),
    )
    report = describe_environment(spec)
    return {
        "requirements_satisfied": report["requirements_satisfied"],
        "problems": report["problems"],
    }


def command(
    python: str,
    row: dict,
    *,
    output: Path,
    num_tasks: int,
    image_aliases: Path | None = None,
    resource_overrides: dict[str, float] | None = None,
) -> list[str]:
    argv = [
        python,
        str(Path(__file__).with_name("prime_validation_entrypoint.py").resolve()),
        str(image_aliases.resolve()) if image_aliases is not None else "-",
        row["taskset"],
        "--qualification-evidence",
        str(output / f"{row['taskset']}-runtimes.jsonl"),
        "--runtime.type",
        "ucloud",
        "--output-dir",
        str(output),
        "--run.dir",
        row["taskset"],
        "--max-concurrent",
        "1",
        "--timeout.setup",
        "900",
        "--timeout.total",
        "1800",
    ]
    for resource, value in (resource_overrides or {}).items():
        argv.extend(
            [
                f"--runtime.{resource}",
                str(value),
                f"--qualification-{resource}",
                str(value),
            ]
        )
    if num_tasks:
        argv.extend(["-n", str(num_tasks)])
    if row["mode"] == "setup":
        argv.append("--only-setup")
    return argv


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python with pinned tasksets, verifiers and verifiers-ucloud installed",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New directory for plan, logs and verdicts",
    )
    parser.add_argument(
        "--num-tasks",
        type=int,
        default=1,
        help="Per taskset; 0 selects the entire dataset",
    )
    parser.add_argument(
        "--taskset",
        action="append",
        help="Explicit subset; omitted means all 23 families",
    )
    parser.add_argument(
        "--image-aliases",
        type=Path,
        help="Explicit taskset-scoped public image mappings; recorded in the plan",
    )
    parser.add_argument("--cpu", type=float, help="Require this resolved CPU limit")
    parser.add_argument(
        "--memory-gib", type=float, help="Require this resolved memory limit in GiB"
    )
    parser.add_argument(
        "--disk-gib",
        type=float,
        help="Require this resolved writable disk limit in GiB",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Provision sandboxes and run upstream validation",
    )
    args = parser.parse_args()
    if args.num_tasks < 0:
        parser.error("--num-tasks cannot be negative")
    resource_overrides = {
        key: value
        for key, value in (
            ("cpu", args.cpu),
            ("memory", args.memory_gib),
            ("disk", args.disk_gib),
        )
        if value is not None
    }
    if any(
        not math.isfinite(value) or value <= 0 for value in resource_overrides.values()
    ):
        parser.error("resource overrides must be finite and positive")
    manifest = json.loads(MANIFEST.read_text())
    verify_sources(args.source.resolve(), manifest)
    selected = set(args.taskset or [row["taskset"] for row in manifest["tasksets"]])
    if selected - {row["taskset"] for row in manifest["tasksets"]}:
        parser.error("unknown taskset selector")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    rows = [row for row in manifest["tasksets"] if row["taskset"] in selected]
    plan = [
        {
            "taskset": row["taskset"],
            "mode": row["mode"],
            "preflight": runtime_preflight(row),
            "command": command(
                args.python,
                row,
                output=output,
                num_tasks=args.num_tasks,
                image_aliases=args.image_aliases,
                resource_overrides=resource_overrides,
            ),
        }
        for row in rows
    ]
    (output / "plan.json").write_text(
        json.dumps(
            {
                "image_aliases": json.loads(args.image_aliases.read_text())
                if args.image_aliases
                else None,
                "source_commit": manifest["source_commit"],
                "sample_size": args.num_tasks or "all",
                "resource_overrides": resource_overrides,
                "checks": plan,
            },
            indent=2,
        )
        + "\n"
    )
    if not args.execute:
        print(
            f"Wrote {len(plan)} checks to {output / 'plan.json'}; no sandboxes created"
        )
        return 0
    runnable = [
        row
        for row, check in zip(rows, plan)
        if check["preflight"]["requirements_satisfied"]
    ]
    if runnable:
        verify_installed_sources(args.python, args.source.resolve(), runnable)
    results = []
    for row, check in zip(rows, plan):
        if not check["preflight"]["requirements_satisfied"]:
            results.append(
                {
                    "taskset": row["taskset"],
                    "status": "blocked_preflight",
                    "preflight": check["preflight"],
                }
            )
            (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            print(f"Blocked before setup: {row['taskset']}", flush=True)
            continue
        print(f"Checking {row['taskset']} ({row['mode']})", flush=True)
        with (output / f"{row['taskset']}.log").open("w") as log:
            completed = subprocess.run(
                check["command"], stdout=log, stderr=subprocess.STDOUT, check=False
            )
        summary_path = output / row["taskset"] / "summary.json"
        try:
            summary = json.loads(summary_path.read_text())
            status = verdict(
                summary, mode=row["mode"], expected_total=args.num_tasks or None
            )
        except (OSError, ValueError, TypeError):
            summary, status = None, "failed_or_incomplete"
        if completed.returncode:
            status = "failed_or_incomplete"
        results.append(
            {
                "taskset": row["taskset"],
                "status": status,
                "exit_code": completed.returncode,
                "summary": summary,
            }
        )
        (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    return int(
        any(row["status"] not in {"setup_passed", "sample_passed"} for row in results)
    )


if __name__ == "__main__":
    raise SystemExit(main())
