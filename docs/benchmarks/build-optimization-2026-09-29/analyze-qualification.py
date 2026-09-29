#!/usr/bin/env python3
"""Compare downloaded build summaries without contacting any service.

Print JSON by default. --output-prefix writes .json and .md reports. Inputs
are never changed; this does not qualify sandbox semantics or deployment.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("build_load_report", ROOT / "scripts/build_load_report.py")
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)
ENVIRONMENT_COUNTERS = (
    "groups_reused", "groups_built", "erofs_bytes_built", "preflight_misses",
    "docker_pull_skipped", "selective_materializations", "selective_fallbacks",
    "oci_layers_materialized", "oci_download_bytes",
)
LIMITATIONS = [
    "This historical comparison is descriptive: context sets, cache warmth, fleet age and release changes can confound latency deltas.",
    "Admission is created_at to finished_at, including preparation; execution is execution_started_at to finished_at, including publication but excluding later cleanup.",
    "Nonzero queue time can reflect cleanup handoff. Qualification checks the four-nonterminal admission limit, not literal zero queue_wait_ms.",
    "An unused measured execution slot does not prove memory/disk/cache readiness or quantify avoidable client delay. Cross-owner timestamps require synchronized clocks.",
    "Gateway admission waiting is part of submission/client latency and is not included in the node queue intervals.",
    "Missing environment counters are unreported, not zero. OCI bytes count selected compressed descriptors, not measured physical registry I/O.",
    "HTTP 503 counts do not identify error_code when the harness did not capture response bodies. Retry counts alone do not decide acceptance.",
    "Durable history completeness uses the harness's per-build lookup count; this report does not independently query the database or validate row contents.",
    "This report does not establish real-sandbox correctness, source/bundle identity, host resource headroom, or fleet-wide production capacity.",
]


def load_summary(path):
    if path.stat().st_size > 64 * 1024 * 1024:
        raise ValueError(f"summary exceeds 64 MiB: {path}")
    result = json.loads(path.read_text())
    if not isinstance(result, dict) or not isinstance(result.get("records"), list):
        raise ValueError(f"summary must contain a records list: {path}")
    if len(result["records"]) > 256 or any(not isinstance(r, dict) for r in result["records"]):
        raise ValueError(f"expected at most 256 record objects: {path}")
    return result


def append_span(spans, start, end):
    if spans and spans[-1][1] == start:
        spans[-1][1] = end
    else:
        spans.append([start, end])


def span_summary(spans):
    longest = max(spans, key=lambda pair: pair[1] - pair[0], default=None)
    return {
        "total_seconds": sum(end - start for start, end in spans),
        "longest_seconds": longest[1] - longest[0] if longest else 0,
        "longest_started_at": REPORT.iso(longest[0]) if longest else None,
        "longest_finished_at": REPORT.iso(longest[1]) if longest else None,
        "spans": [{"started_at": REPORT.iso(start), "finished_at": REPORT.iso(end),
                   "seconds": end - start} for start, end in spans],
    }


def queue_analysis(summary, slots):
    records = summary["records"]
    owners = {REPORT.owner(r) for r in records} - {"unknown"}
    for node in summary.get("fleet_after", []):
        if (isinstance(node, dict) and "image-build" in node.get("capabilities", [])
                and "sandbox" not in node.get("capabilities", []) and node.get("job_id")):
            owners.add(str(node["job_id"]))
    rows, missing, duplicate, seen = [], [], [], set()
    events = defaultdict(list)
    for record in records:
        build = record.get("build") or {}
        build_id = build.get("build_id") or record.get("build_id")
        row = {"index": record.get("index"), "build_id": build_id,
               "recipe": record.get("recipe"), "owner": REPORT.owner(record)}
        if build_id and build_id in seen:
            duplicate.append(build_id)
            continue
        if build_id:
            seen.add(build_id)
        stamps = [REPORT.timestamp(build.get(key)) for key in
                  ("created_at", "queued_at", "execution_started_at", "finished_at")]
        if (not build_id or row["owner"] == "unknown" or any(t is None for t in stamps)
                or stamps != sorted(stamps)):
            missing.append(row)
            continue
        created, queued, execution, finished = stamps
        row.update(created_at=REPORT.iso(created), queued_at=REPORT.iso(queued),
                   execution_started_at=REPORT.iso(execution), finished_at=REPORT.iso(finished),
                   queue_seconds_from_timestamps=execution - queued,
                   reported_queue_ms=REPORT.nested(build, ("timings", "queue_wait_ms")))
        rows.append(row)
        for begin, end, kind in ((created, finished, "admitted"),
                                 (queued, execution, "queued"),
                                 (execution, finished, "executing")):
            if begin < end:
                events[begin].append((row["owner"], kind, 1))
                events[end].append((row["owner"], kind, -1))
    states = {owner: Counter() for owner in owners}
    any_queue_spans, full_queue_spans = [], []
    affected_seconds = 0.0
    boundaries = sorted(events)
    for index, start in enumerate(boundaries[:-1]):
        # Apply all starts/ends at once, yielding half-open interval state.
        for owner, kind, change in events[start]:
            states[owner][kind] += change
        end = boundaries[index + 1]
        affected_owners = [owner for owner, state in states.items()
                           if state["queued"] and any(other != owner and peer["executing"] < slots
                                                      for other, peer in states.items())]
        if affected_owners:
            append_span(any_queue_spans, start, end)
        full = [owner for owner in affected_owners if states[owner]["executing"] >= slots]
        if full:
            append_span(full_queue_spans, start, end)
            affected_seconds += (end - start) * sum(states[owner]["queued"] for owner in full)
    return {
        "owners_considered": sorted(owners),
        "owners_include_fleet_after": True,
        "complete_intervals": len(rows), "missing_or_invalid_intervals": missing,
        "duplicate_build_ids_ignored": duplicate,
        "coverage_complete": len(rows) == len(records),
        "queued_with_free_peer": span_summary(any_queue_spans),
        "queued_behind_full_owner_with_free_peer": {
            **span_summary(full_queue_spans), "affected_queued_build_seconds": affected_seconds},
        "build_intervals": rows,
    }


def environment_metrics(records):
    result = {}
    keys = set(ENVIRONMENT_COUNTERS)
    for record in records:
        raw = REPORT.nested(record, ("build", "timings", "environment"))
        if isinstance(raw, dict):
            keys.update(key for key in raw if key.endswith("_ms"))
    for key in sorted(keys):
        values = [REPORT.nested(record, ("build", "timings", "environment", key)) for record in records]
        values = [value for value in values if REPORT.number(value)]
        result[key] = {"records_reporting": len(values), "records_positive": sum(v > 0 for v in values),
                       "total": sum(values) if values else None, "distribution": REPORT.distribution(values)}
    return result


def analyze(path, *, slots):
    summary = load_summary(path)
    aggregated = REPORT.aggregate(summary["records"])
    queue = queue_analysis(summary, slots)
    reported_history = summary.get("durable_history_records")
    history_known = type(reported_history) is int and 0 <= reported_history <= aggregated["cases"]
    return {
        "path": str(path.resolve()), "phase": summary.get("phase"),
        "batch_wall_seconds": summary.get("batch_wall_seconds"),
        "concurrency": summary.get("concurrency"),
        "gateway_loopback_client": summary.get("gateway_loopback_client"),
        **aggregated, "queue_fairness": queue,
        "environment": environment_metrics(summary["records"]),
        "durable_history": {"reported_records": reported_history,
            "complete": (reported_history == aggregated["cases"]
                         and aggregated["unique_build_ids"] == aggregated["cases"])
                        if history_known else None},
        "fixture_hashes": sorted({r["context_sha256"] for r in summary["records"] if r.get("context_sha256")}),
    }


def capacity_gate(phase, kind, limit):
    value = phase["overlap"][kind]
    known = (value["complete_intervals"] == phase["cases"]
             and not value["duplicate_build_ids_ignored"]
             and "unknown" not in value["by_owner"])
    return all(v["peak"] <= limit for v in value["by_owner"].values()) if known else None


def compare(baseline, candidate, *, cases, builders, slots, admission_limit):
    deltas = {}
    pairs = {"batch_wall_seconds": (baseline["batch_wall_seconds"], candidate["batch_wall_seconds"])}
    for metric in ("client_wall_seconds", "submission_seconds", "queue_seconds",
                   "build_push_seconds", "erofs_publication_seconds"):
        for quantile in ("p50", "p95", "max"):
            pairs[f"{metric}.{quantile}"] = tuple(phase["measurements"][metric][quantile]
                                                   for phase in (baseline, candidate))
    for name, (before, after) in pairs.items():
        deltas[name] = {"baseline": before, "candidate": after,
            "candidate_minus_baseline": after - before if REPORT.number(before) and REPORT.number(after) else None,
            "candidate_over_baseline": after / before if REPORT.number(before) and before > 0 and REPORT.number(after) else None}
    return {
        "schema_version": 1, "baseline": baseline, "candidate": candidate,
        "observed_deltas_not_causal": deltas,
        "same_fixture_hash_set": baseline["fixture_hashes"] == candidate["fixture_hashes"],
        "candidate_evidence_gates": {
            "expected_cases": candidate["cases"] == cases,
            "all_succeeded": candidate["statuses"].get("succeeded", 0) == cases and not candidate["client_errors"],
            "unique_build_ids": candidate["unique_build_ids"] == cases and not candidate["duplicate_build_ids"],
            "complete_client_timings": candidate["measurements"]["client_wall_seconds"]["count"] == cases,
            "expected_concurrency": candidate["concurrency"] == cases,
            "expected_builder_owners": candidate["unique_known_owners"] == builders,
            "maximum_admitted_per_owner": capacity_gate(candidate, "admitted", admission_limit),
            "maximum_executing_per_owner": capacity_gate(candidate, "executing", slots),
            "durable_history_complete": candidate["durable_history"]["complete"],
            "queue_intervals_complete": candidate["queue_fairness"]["coverage_complete"],
        },
        "limits": {"expected_cases": cases, "expected_builders": builders,
                   "execution_slots_per_builder": slots, "maximum_admitted_per_builder": admission_limit},
        "limitations": LIMITATIONS,
    }


def fmt(value):
    return "unknown" if value is None else f"{value:.3f}" if isinstance(value, float) else str(value)


def markdown(result):
    baseline, candidate = result["baseline"], result["candidate"]
    lines = ["# Build optimization: observed comparison", "",
             "Historical baseline comparison; observed changes are not isolated causal speedups.", "",
             f"Baseline: `{baseline['path']}`  ", f"Candidate: `{candidate['path']}`", "",
             "| Measurement | Baseline | Candidate |", "| --- | ---: | ---: |"]
    def row(label, get):
        lines.append(f"| {label} | {fmt(get(baseline))} | {fmt(get(candidate))} |")
    row("Successful / cases", lambda p: f"{p['statuses'].get('succeeded', 0)} / {p['cases']}")
    row("Batch seconds", lambda p: p["batch_wall_seconds"])
    for name, label in (("client_wall_seconds", "Client"), ("submission_seconds", "Submission"),
                        ("queue_seconds", "Node queue"), ("build_push_seconds", "Build/push"),
                        ("erofs_publication_seconds", "EROFS publication")):
        for quantile in ("p50", "p95"):
            row(f"{label} {quantile} seconds", lambda p, n=name, q=quantile: p["measurements"][n][q])
    for kind in ("admitted", "executing"):
        row(f"Fleet peak {kind}", lambda p, k=kind: p["overlap"][k]["peak"])
        row(f"Maximum per-owner {kind}", lambda p, k=kind: max((v["peak"] for v in p["overlap"][k]["by_owner"].values()), default=None))
    row("HTTP 503 responses", lambda p: p["http"]["responses_503"])
    row("Repeated submit attempts", lambda p: p["http"]["repeated_submit_attempts"])
    row("History rows reported", lambda p: p["durable_history"]["reported_records"])
    for metric, label in (("longest_seconds", "Longest"), ("total_seconds", "Total")):
        row(f"{label} queue behind full owner with free peer (seconds)",
            lambda p, m=metric: p["queue_fairness"]["queued_behind_full_owner_with_free_peer"][m])
    for key in ENVIRONMENT_COUNTERS:
        row(f"{key} total (records reporting)", lambda p, k=key:
            f"{fmt(p['environment'][k]['total'])} ({p['environment'][k]['records_reporting']})")
    lines += ["", "## Candidate evidence gates", "", "These gates do not replace the live semantic and resource checks.", ""]
    for key, value in result["candidate_evidence_gates"].items():
        lines.append(f"- {key}: {'unknown' if value is None else 'pass' if value else 'FAIL'}")
    lines += ["", f"Same fixture hash set: {result['same_fixture_hash_set']}.", "",
              "Exact per-owner peaks, build intervals, queue spans, cache-step evidence, HTTP categories,",
              "environment distributions and failures are retained in the companion JSON.", "", "## Interpretation limits", ""]
    lines.extend(f"- {text}" for text in result["limitations"])
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--baseline", type=Path, default=ROOT / "docs/benchmarks/build-load-2026-09-29/overload/summary.json")
    parser.add_argument("--output-prefix", type=Path)
    parser.add_argument("--expected-cases", type=int, default=48)
    parser.add_argument("--expected-builders", type=int, default=4)
    parser.add_argument("--execution-slots", type=int, default=4)
    parser.add_argument("--max-admitted-per-owner", type=int, default=4)
    args = parser.parse_args()
    if any(value < 1 or value > 256 for value in (args.expected_cases, args.expected_builders,
                                                 args.execution_slots, args.max_admitted_per_owner)):
        parser.error("counts must be between 1 and 256")
    result = compare(analyze(args.baseline, slots=args.execution_slots),
                     analyze(args.candidate, slots=args.execution_slots),
                     cases=args.expected_cases, builders=args.expected_builders,
                     slots=args.execution_slots, admission_limit=args.max_admitted_per_owner)
    serialized = json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if args.output_prefix:
        args.output_prefix.with_suffix(".json").write_text(serialized)
        args.output_prefix.with_suffix(".md").write_text(markdown(result))
    else:
        print(serialized, end="")


if __name__ == "__main__":
    main()
