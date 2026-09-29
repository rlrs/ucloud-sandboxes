#!/usr/bin/env python3
"""Derive nested preparation costs and health-probe status from local receipts."""
from __future__ import annotations

import argparse
from collections import Counter
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("load_report", ROOT / "scripts/build_load_report.py")
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)
PHASES = ("cache_prepare_ms", "cache_mount_ms", "docker_build_and_push_ms", "immutable_environment_ms")
ENVIRONMENT = ("selective_materialization_ms", "squash_ms", "mkfs_ms", "sign_ms",
               "publish_component_ms", "component_lookup_ms", "layer_lock_wait_ms", "preflight_ms")
LIMITATIONS = [
    "Historical runs use identical context hashes but different placement/cache history; deltas are descriptive, not isolated causal speedups.",
    "Durations are milliseconds with linearly interpolated percentiles. Missing values remain missing; they are not zero.",
    "Cache preparation/mount are nested inside build-and-push. Preflight contains selective extraction, squash and other work. Do not sum parent and child timings or independently calculated percentiles.",
    "Selective materialization includes registry download, authentication, decompression, validation and extraction; it is not an extraction-only timer.",
    "Per-build extraction-plus-squash sums disjoint stages before aggregation; summed wall time across concurrent builds is neither batch elapsed time nor CPU time.",
    "Sampler health status reflects its configured endpoint. A protected endpoint's HTTP401 response cannot establish successful health during the burst.",
]


def checksum(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def timing(records, section, key):
    values = [REPORT.nested(record, ("build", "timings", section, key)) for record in records]
    values = [value for value in values if REPORT.number(value)]
    return {"records_reporting": len(values), "records_expected": len(records),
            "total_ms": sum(values) if values else None, **REPORT.distribution(values)}


def describe(records):
    combined = []
    for record in records:
        values = [REPORT.nested(record, ("build", "timings", "environment", name))
                  for name in ("selective_materialization_ms", "squash_ms")]
        if all(REPORT.number(value) for value in values):
            combined.append(sum(values))
    return {"records": len(records), "phases": {name: timing(records, "phases", name) for name in PHASES},
            "environment": {name: timing(records, "environment", name) for name in ENVIRONMENT},
            "extraction_plus_squash_ms": {"records_reporting": len(combined),
                "records_expected": len(records), "total_ms": sum(combined) if combined else None,
                **REPORT.distribution(combined)}}


def summarize(path):
    source = json.loads(path.read_text())
    records = source["records"]
    return {"source": str(path.relative_to(ROOT)), "sha256": checksum(path), "phase": source["phase"],
            "batch_wall_seconds": source["batch_wall_seconds"], "all": describe(records),
            "by_recipe": {recipe: describe([record for record in records if record["recipe"] == recipe])
                          for recipe in sorted({record["recipe"] for record in records})}}


def health(path, summary_path):
    source = json.loads(summary_path.read_text())
    begin = min(REPORT.timestamp(record["started_at"]) for record in source["records"])
    end = max(REPORT.timestamp(record["finished_at"]) for record in source["records"])
    statuses = Counter()
    metadata = None
    with gzip.open(path, "rt") as stream:
        for line in stream:
            row = json.loads(line)
            if row.get("type") == "metadata":
                metadata = row
            sample = row.get("health") or {}
            at = REPORT.timestamp(sample.get("at"))
            if at is not None and begin <= at <= end:
                statuses[str(sample.get("status", "no_status"))] += 1
    return {"source": str(path.relative_to(ROOT)), "sha256": checksum(path),
            "window": {"started_at": REPORT.iso(begin), "finished_at": REPORT.iso(end)},
            "health_url": (metadata or {}).get("health_url"), "response_status_counts": dict(statuses),
            "interpretation": "HTTP401 from /health is an invalid successful-health gate; retain raw failures and use independent correctly configured checks."}


def fmt(value):
    return "unreported" if value is None else f"{value:.3f}"


def triple(value):
    return " / ".join(fmt(value[key]) for key in ("p50", "p95", "max"))


def markdown(report):
    lines = ["# Builder preparation: retained phase costs", "",
             "All durations below are milliseconds, shown as median / p95 / maximum. Parent and child timers overlap.", "",
             "| Nested build phase | Baseline reporting | Baseline | Candidate reporting | Candidate |",
             "| --- | ---: | ---: | ---: | ---: |"]
    for key in PHASES:
        before, after = [report[arm]["all"]["phases"][key] for arm in ("baseline", "candidate")]
        lines.append(f"| {key} | {before['records_reporting']}/48 | {triple(before)} | {after['records_reporting']}/48 | {triple(after)} |")
    for recipe in ("all", *report["candidate"]["by_recipe"]):
        lines += ["", f"## {recipe}", "", "| Environment subphase | Baseline | Candidate | Candidate reporting |",
                  "| --- | ---: | ---: | ---: |"]
        selected = [report[arm]["all"] if recipe == "all" else report[arm]["by_recipe"][recipe]
                    for arm in ("baseline", "candidate")]
        for key in (*ENVIRONMENT, "extraction_plus_squash_ms"):
            before, after = [item[key] if key == "extraction_plus_squash_ms" else item["environment"][key]
                             for item in selected]
            lines.append(f"| {key} | {triple(before)} | {triple(after)} | {after['records_reporting']}/{after['records_expected']} |")
    probe = report["candidate_health"]
    lines += ["", "## Health-probe interpretation", "",
              f"Configured endpoint: `{probe['health_url']}`. Within the client window, status counts were `{probe['response_status_counts']}`.",
              probe["interpretation"], "", "## Limits", ""]
    lines.extend(f"- {text}" for text in report["limitations"])
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, default=ROOT / "docs/benchmarks/registry-io-2026-09-29/io-repeat/summary.json")
    parser.add_argument("--candidate", type=Path, default=HERE / "prep-repeat/summary.json")
    parser.add_argument("--telemetry", type=Path, default=HERE / "telemetry/gateway.jsonl.gz")
    parser.add_argument("--output-prefix", type=Path, default=HERE / "phase-costs")
    args = parser.parse_args()
    inputs = tuple(path.resolve() for path in (args.baseline, args.candidate, args.telemetry))
    outputs = tuple(args.output_prefix.with_suffix(suffix).resolve() for suffix in (".json", ".md"))
    if any(path in inputs for path in outputs):
        parser.error("output cannot overwrite an input")
    report = {"schema": 1, "unit": "milliseconds", "baseline": summarize(inputs[0]),
              "candidate": summarize(inputs[1]), "candidate_health": health(inputs[2], inputs[1]),
              "limitations": LIMITATIONS}
    outputs[0].write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    outputs[1].write_text(markdown(report))
    print(json.dumps({"json": str(outputs[0]), "markdown": str(outputs[1])}))


if __name__ == "__main__":
    main()
