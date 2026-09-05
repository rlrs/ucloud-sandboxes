#!/usr/bin/env python3
"""Plan or run pinned upstream taskset checks; never infer success from CLI exit."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys


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


def verdict(summary: dict, *, mode: str) -> str:
    total = summary.get("total", 0)
    outcomes = summary.get("outcomes", {})
    if (
        not isinstance(total, int)
        or total < 1
        or summary.get("recorded") != total
        or summary.get("mode") != mode
        or any(
            outcomes.get(key, 0)
            for key in ("invalid", "error", "timeout", "missing", "unchecked")
        )
        or outcomes.get("valid") != total
    ):
        return "failed_or_incomplete"
    if mode == "all":
        for check in ("gold", "setup"):
            if summary.get("checks", {}).get(check, {}).get("valid") != total:
                return "failed_or_incomplete"
    return "setup_passed" if mode == "setup" else "sample_passed"


def command(
    python: str,
    row: dict,
    *,
    output: Path,
    num_tasks: int,
    image_aliases: Path | None = None,
) -> list[str]:
    argv = [
        python,
        "-m",
        "verifiers.v1.cli.validate",
        row["taskset"],
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
    if image_aliases is not None:
        argv[1:3] = [
            str(Path(__file__).with_name("prime_validation_entrypoint.py").resolve()),
            str(image_aliases.resolve()),
        ]
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
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Provision sandboxes and run upstream validation",
    )
    args = parser.parse_args()
    if args.num_tasks < 0:
        parser.error("--num-tasks cannot be negative")
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
            "command": command(
                args.python,
                row,
                output=output,
                num_tasks=args.num_tasks,
                image_aliases=args.image_aliases,
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
    verify_installed_sources(args.python, args.source.resolve(), rows)
    results = []
    for row, check in zip(rows, plan):
        print(f"Checking {row['taskset']} ({row['mode']})", flush=True)
        with (output / f"{row['taskset']}.log").open("w") as log:
            completed = subprocess.run(
                check["command"], stdout=log, stderr=subprocess.STDOUT, check=False
            )
        summary_path = output / row["taskset"] / "summary.json"
        try:
            summary = json.loads(summary_path.read_text())
            status = verdict(summary, mode=row["mode"])
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
    return int(any(row["status"] == "failed_or_incomplete" for row in results))


if __name__ == "__main__":
    raise SystemExit(main())
