#!/usr/bin/env python3
"""Analyze local live_build_load_benchmark artifacts; never contact production.

Usage: python3 scripts/build_load_report.py --root <downloaded-artifact-root>
Optional telemetry lives in root/telemetry/*.jsonl[.gz] or --telemetry PATH entries.
Latency percentiles interpolate observations; telemetry uses its sampler's
nearest-rank p95 and time-weighted means. Missing evidence stays missing.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import redirect_stdout
from datetime import datetime, timezone
from functools import lru_cache
import gzip
import importlib.util
import io
import json
import math
from pathlib import Path
import re


METRICS = {
    "client_wall_seconds": ("client_wall_seconds",),
    "submission_seconds": ("submission_seconds",),
    "polling_seconds": ("polling_seconds",),
    "preparation_seconds": ("build", "timings", "preparation_ms"),
    "queue_seconds": ("build", "timings", "queue_wait_ms"),
    "execution_seconds": ("build", "timings", "total_ms"),
    "worker_end_to_end_seconds": ("build", "timings", "end_to_end_ms"),
    "build_push_seconds": ("build", "timings", "phases", "docker_build_and_push_ms"),
    "erofs_publication_seconds": ("build", "timings", "phases", "immutable_environment_ms"),
    "cleanup_seconds": ("build", "timings", "phases", "cleanup_ms"),
}
LIMITATIONS = [
    "Client latency includes SDK compression/upload and gateway admission; worker queue time begins after context materialization.",
    "Execution overlap spans execution_started_at to finished_at: it includes build/push and EROFS publication, not only RUN instructions, and excludes subsequent cleanup.",
    "Overlap is computed from complete, unique build intervals. Missing intervals make the observed peak a lower bound; timestamps across hosts depend on synchronized clocks.",
    "CACHED evidence proves only the described vertex. It may follow waiting for a concurrently computed stage and does not prove the request avoided compilation. Missing/truncated evidence is unknown, not a miss; a warm local hit does not prove cross-builder sharing.",
    "Repeated submit HTTP attempts and 503s are transport observations, not duplicate execution. Context GET 404s are normal cache lookup misses.",
    "Fixture hashes exclude fixture.json and differ from SDK archive hashes. Repeated fixtures can share/coalesce BuildKit graphs even with unique image/build IDs.",
    "EROFS bytes built exclude reused components and are neither total image size nor registry physical growth.",
    "Gateway-local drivers consume gateway CPU. Driver process attribution is separate but incomplete for short-lived processes; subtracting process p95 from host p95 is invalid.",
    "Disk devices and network interfaces are separate accounting views; their rates must not be summed. Health probes originate on the sampled host.",
    "Disk busy counters exceeding 105% over an interval are flagged as unreliable utilization (possible delayed/batched counter accounting). Raw derived statistics are retained in JSON, without clamping.",
    "Record UTC timestamps have one-second precision. Telemetry includes only complete intervals within that recorded client window; latency distributions use all records with that measurement, including failures.",
]


def number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def nested(value, keys):
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.timestamp() if parsed.tzinfo else None
    except (ValueError, OverflowError):
        return None


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def distribution(values):
    values = sorted(value for value in values if number(value))

    def percentile(fraction):
        if not values:
            return None
        position = (len(values) - 1) * fraction
        lower = int(position)
        return values[lower] + (values[min(lower + 1, len(values) - 1)] - values[lower]) * (position - lower)

    return {"count": len(values), "min": min(values, default=None),
            "mean": sum(values) / len(values) if values else None,
            "p50": percentile(.5), "p95": percentile(.95), "max": max(values, default=None)}


def owner(record):
    build = record.get("build") or {}
    node = build.get("node") or {}
    return str(node.get("job_id") or node.get("node_id") or build.get("location") or "unknown")


def overlap(records, start_field):
    events, per_owner, seen = [], defaultdict(list), set()
    missing, duplicate, valid = 0, 0, 0
    for record in records:
        build = record.get("build") or {}
        build_id = build.get("build_id") or record.get("build_id")
        if not build_id:
            missing += 1
            continue
        if build_id in seen:
            duplicate += 1
            continue
        seen.add(build_id)
        begin, end = timestamp(build.get(start_field)), timestamp(build.get("finished_at"))
        if begin is None or end is None or end < begin:
            missing += 1
            continue
        valid += 1
        if end == begin:
            continue
        interval = [(begin, 1), (end, -1)]
        events.extend(interval)
        per_owner[owner(record)].extend(interval)

    def peak(points):
        current = maximum = 0
        at = None
        for when, change in sorted(points):  # End before start: half-open intervals.
            current += change
            if current > maximum:
                maximum, at = current, when
        return {"peak": maximum, "first_peak_at": iso(at) if at is not None else None}

    return {**peak(events), "complete_intervals": valid, "missing_or_invalid_intervals": missing,
            "duplicate_build_ids_ignored": duplicate, "by_owner": {key: peak(value) for key, value in sorted(per_owner.items())}}


def cache_category(description):
    if not description:
        return "description_missing"
    if re.search(r"\bRUN\b.*(?:npm ci|pip install)", description):
        return "dependency_install"
    if re.search(r"\bRUN\b.*(?:npm run build|compileall)", description):
        return "application_compile"
    if re.search(r"\bCOPY\b", description):
        return "copy"
    return "other"


def cache_evidence(records):
    descriptions, categories = Counter(), Counter()
    evidence, critical = [], []
    for record in records:
        steps = record.get("cached_steps_observed") or []
        observed = {(str(step.get("id", "")), step.get("description"))
                    for step in steps if isinstance(step, dict)}
        if observed:
            evidence.append(record.get("build_id") or record.get("image_id"))
        found_categories = set()
        for _step_id, description in observed:
            descriptions[description or "(description unavailable)"] += 1
            category = cache_category(description)
            found_categories.add(category)
            if record.get("recipe") == "typescript-multistage" and category == "application_compile":
                critical.append({"build_id": record.get("build_id"), "owner": owner(record),
                                 "description": description})
        categories.update(found_categories)
    return {"records_with_observed_cached_steps": len(evidence),
            "records_without_observed_cached_steps": len(records) - len(evidence),
            "records_by_step_category": dict(categories),
            "step_descriptions": dict(sorted(descriptions.items())),
            "multistage_compile_hits": critical,
            "multistage_records_with_compile_hit": len({item["build_id"] for item in critical})}


def aggregate(records):
    measurements = {}
    for name, path in METRICS.items():
        values = [nested(record, path) for record in records]
        measurements[name] = distribution(value / (1000 if path[-1].endswith("_ms") else 1)
                                          for value in values if number(value))
    statuses, by_category, latencies = Counter(), defaultdict(Counter), defaultdict(list)
    repeated_submits = cases_503 = 0
    for record in records:
        events = [event for event in record.get("http", []) if isinstance(event, dict)]
        submit_count = 0
        cases_503 += any(event.get("status") == 503 for event in events)
        for event in events:
            status, category = str(event.get("status")), str(event.get("category", "unknown"))
            statuses[status] += 1
            by_category[category][status] += 1
            submit_count += category == "submit" and event.get("method") == "POST"
            if number(event.get("headers_seconds")):
                latencies[category].append(event["headers_seconds"])
        repeated_submits += max(0, submit_count - 1)
    states = Counter(str(nested(record, ("build", "status")) or "unobserved") for record in records)
    ids = [nested(record, ("build", "build_id")) or record.get("build_id") for record in records]
    ids = [value for value in ids if value]
    owners = Counter(owner(record) for record in records if record.get("build_id") or record.get("build"))
    environment = {}
    for key in ("groups_reused", "groups_built", "erofs_bytes_built", "docker_pull_skipped"):
        values = [nested(record, ("build", "timings", "environment", key)) for record in records]
        reported = [value for value in values if number(value)]
        environment[key] = {"records_reporting": len(reported), "total": sum(reported) if reported else None}
    return {"cases": len(records), "statuses": dict(states),
            "failed_or_incomplete": len(records) - states["succeeded"],
            "client_errors": sum(bool(record.get("exception_type")) for record in records),
            "refresh_errors": sum(bool(record.get("refresh_error_type")) for record in records),
            "unique_build_ids": len(set(ids)), "duplicate_build_ids": len(ids) - len(set(ids)),
            "owners": dict(sorted(owners.items())), "unique_known_owners": len(set(owners) - {"unknown"}),
            "distinct_fixture_hashes": len({record["context_sha256"] for record in records if record.get("context_sha256")}),
            "variants": dict(Counter(str(record.get("variant", "unknown")) for record in records)),
            "measurements": measurements,
            "overlap": {"admitted": overlap(records, "created_at"), "executing": overlap(records, "execution_started_at")},
            "http": {"statuses": dict(statuses), "by_category": {key: dict(value) for key, value in by_category.items()},
                     "responses_503": statuses["503"], "cases_with_503": cases_503,
                     "repeated_submit_attempts": repeated_submits,
                     "time_to_headers_seconds": {key: distribution(value) for key, value in sorted(latencies.items())}},
            "cache": cache_evidence(records), "environment": environment,
            "failures": [{key: record.get(key) for key in ("index", "recipe", "variant", "image_id", "build_id", "exception_type", "error")}
                         for record in records if nested(record, ("build", "status")) != "succeeded" or record.get("exception_type")]}


class GzipInput:
    """Read-only path adapter for the existing telemetry summarizer."""

    def __init__(self, path):
        self.path = path

    def open(self, *, encoding):
        return gzip.open(self.path, "rt", encoding=encoding)


def discover_telemetry(root):
    # During compression both copies may exist. Prefer the plain copy once,
    # rather than reporting one host twice; explicit --telemetry stays literal.
    paths = {path.with_suffix(""): path for path in root.glob("*.jsonl.gz")}
    paths.update({path: path for path in root.glob("*.jsonl")})
    return [paths[key] for key in sorted(paths)]


@lru_cache(maxsize=64)
def telemetry_lifespan(path, modified_ns, size):
    """Cache the complete observed sample span; stat arguments invalidate it."""
    first = last = None
    samples = skipped = 0
    source = GzipInput(path) if path.suffix == ".gz" else path
    with source.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            if not isinstance(row, dict) or row.get("type") != "sample":
                continue
            at = timestamp(row.get("at"))
            if at is None:
                continue
            first = at if first is None else min(first, at)
            last = at if last is None else max(last, at)
            samples += 1
    return {"first_sample": iso(first) if first is not None else None,
            "last_sample": iso(last) if last is not None else None,
            "samples": samples, "malformed_lines": skipped}


def outside_phase_window(host, window, expected_owners):
    label = str((host.get("metadata") or {}).get("label") or "")
    if (not label.startswith("builder-") or host.get("coverage_fraction") != 0
            or label.removeprefix("builder-") in expected_owners):
        return False
    lifespan = host.get("observed_lifespan") or {}
    values = [timestamp(value) for value in (lifespan.get("first_sample"), lifespan.get("last_sample"),
                                             window.get("started_at"), window.get("finished_at"))]
    if any(value is None for value in values):
        return False
    first, last, start, finish = values
    return last < start or first > finish


def telemetry_report(paths, window, expected_owners=()):
    if not paths or window is None:
        return []
    spec = importlib.util.spec_from_file_location("build_load_telemetry", Path(__file__).with_name("build_load_telemetry.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    reports = []
    for path in paths:
        try:
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                source = GzipInput(path) if path.suffix == ".gz" else path
                module.summarize(argparse.Namespace(input=source, output=None, since=window["started_at"], until=window["finished_at"]))
            result = json.loads(buffer.getvalue())
            # Avoid copying individual health observations into every report.
            result["health"].pop("observations", None)
            result["source"] = str(path.resolve())
            result["coverage_fraction"] = result["covered_seconds"] / window["seconds"] if window["seconds"] else None
            stat = path.stat()
            result["observed_lifespan"] = telemetry_lifespan(path, stat.st_mtime_ns, stat.st_size)
            result["relevance"] = "outside_phase_window" if outside_phase_window(result, window, expected_owners) else "included"
            result["metric_warnings"] = [{"metric": key, "reason": "busy counter exceeds elapsed interval; not reliable utilization",
                                          "maximum_percent": value["max"]}
                                         for key, value in result["summary"].items()
                                         if key.startswith("disk/") and key.endswith("/busy_percent") and value["max"] > 105]
            reports.append(result)
        except (OSError, EOFError, ValueError, KeyError, TypeError) as exc:
            reports.append({"source": str(path.resolve()), "error": f"{type(exc).__name__}: {exc}"})
    return reports


def analyze_phase(path, telemetry):
    source = json.loads(path.read_text())
    records = source.get("records")
    if not isinstance(records, list) or any(not isinstance(record, dict) for record in records):
        raise ValueError(f"{path}: records must be an array of objects")
    starts = [timestamp(record.get("started_at")) for record in records]
    ends = [timestamp(record.get("finished_at")) for record in records]
    starts, ends = [value for value in starts if value is not None], [value for value in ends if value is not None]
    window = None
    if starts and ends and max(ends) > min(starts):
        window = {"started_at": iso(min(starts)), "finished_at": iso(max(ends)), "seconds": max(ends) - min(starts),
                  "records_with_start": len(starts), "records_with_finish": len(ends)}
    grouped = defaultdict(list)
    for record in records:
        grouped[str(record.get("recipe", "unknown"))].append(record)
    result = {"phase": source.get("phase", path.parent.name), "source": str(path.resolve()),
              "concurrency": source.get("concurrency"), "batch_wall_seconds": source.get("batch_wall_seconds"),
              "gateway_loopback_client": source.get("gateway_loopback_client"), "window": window,
              "durable_history_records": source.get("durable_history_records"), **aggregate(records),
              "by_recipe": {key: aggregate(value) for key, value in sorted(grouped.items())}}
    wall = result["batch_wall_seconds"]
    result["successful_builds_per_minute"] = result["statuses"].get("succeeded", 0) * 60 / wall if number(wall) and wall else None
    result["telemetry"] = telemetry_report(telemetry, window, result["owners"])
    findings = [f"{result['statuses'].get('succeeded', 0)}/{result['cases']} builds succeeded; "
                f"{result['unique_known_owners']} known builder owners; observed admitted/executing peaks "
                f"{result['overlap']['admitted']['peak']}/{result['overlap']['executing']['peak']}."]
    if result["distinct_fixture_hashes"] < result["cases"]:
        findings.append(f"{result['distinct_fixture_hashes']} distinct recorded fixture hashes for {result['cases']} submissions; repeated graphs can share BuildKit work.")
    if result["http"]["responses_503"]:
        findings.append(f"Observed {result['http']['responses_503']} HTTP 503 responses and {result['http']['repeated_submit_attempts']} repeated submit attempts; use categories to distinguish admission from polling failures.")
    for host in result["telemetry"]:
        if host.get("relevance") == "outside_phase_window":
            continue
        if host.get("error"):
            findings.append(f"Telemetry unavailable for {Path(host['source']).name}: {host['error']}")
        elif host["coverage_fraction"] < .9:
            findings.append(f"{Path(host['source']).name} covers {host['coverage_fraction']:.1%} of the recorded client window; host statistics are partial.")
        if host.get("metric_warnings"):
            findings.append(f"{Path(host['source']).name}: unreliable disk-utilization counters flagged for " + ", ".join(item["metric"] for item in host["metric_warnings"]) + ".")
    result["findings"] = findings
    return result


def cell(value):
    return str(value).replace("|", "\\|").replace("\n", " ")


def fmt(value):
    return f"{value:.3f}" if number(value) else "—"


def markdown(report):
    lines = ["# Build load report", "", "Latency values are seconds. Each cell is p50 / p95; observation counts and maxima are in the JSON report.", "",
             "| Phase / recipe | Success / cases | Client | Submit | Queue | Build + push | EROFS publication | Execution | Admitted / executing peak | Owners |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for phase in report["phases"]:
        for name, value in [(phase["phase"], phase), *[(phase["phase"] + " / " + key, value) for key, value in phase["by_recipe"].items()]]:
            metrics = value["measurements"]
            cells = [cell(name), f"{value['statuses'].get('succeeded', 0)} / {value['cases']}"]
            cells += [fmt(metrics[key]["p50"]) + " / " + fmt(metrics[key]["p95"]) for key in
                      ("client_wall_seconds", "submission_seconds", "queue_seconds", "build_push_seconds", "erofs_publication_seconds", "execution_seconds")]
            cells += [f"{value['overlap']['admitted']['peak']} / {value['overlap']['executing']['peak']}", str(value["unique_known_owners"])]
            lines.append("| " + " | ".join(cells) + " |")
    lines += ["", "## Host telemetry", "", "CPU is occupied cores, not percent. Mean / p95 / maximum refer to sampled intervals; driver CPU is reported separately.", "",
              "| Phase / host | Coverage | Host CPU | Gateway API CPU | Registry CPU | Driver CPU | Health failures / probes | Health p95 ms |",
              "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for phase in report["phases"]:
        for host in phase["telemetry"]:
            if host.get("error") or host.get("relevance") == "outside_phase_window":
                continue
            metrics, health = host["summary"], host["health"]
            cells = [cell(phase["phase"] + " / " + str((host.get("metadata") or {}).get("label") or Path(host["source"]).stem)), f"{host['coverage_fraction']:.1%}"]
            cells += [" / ".join(fmt(metrics.get(key, {}).get(stat)) for stat in ("mean", "p95", "max"))
                      for key in ("cpu/busy_cores", "process/gateway/cpu_cores", "process/registry/cpu_cores", "process/benchmark_driver/cpu_cores")]
            cells += [f"{health['failures']} / {health['probes']}", fmt(health["latency_p95_ms"])]
            lines.append("| " + " | ".join(cells) + " |")
    lines += ["", "## Disk activity", "", "Active devices are separate accounting views: a physical disk and its partition/loop device can describe the same I/O. This table does not attribute all disk traffic to the registry.", "",
              "| Phase / host / device | Read MiB/s p95 / max | Write MiB/s p95 / max | I/O wait ms p95 | Busy % p95 |",
              "|---|---:|---:|---:|---:|"]
    for phase in report["phases"]:
        for host in phase["telemetry"]:
            if host.get("error") or host.get("relevance") == "outside_phase_window":
                continue
            metrics = host["summary"]
            devices = sorted({key.split("/")[1] for key in metrics if key.startswith("disk/")})
            for device in devices:
                prefix = "disk/" + device + "/"
                rates = [metrics.get(prefix + direction + "_bytes_per_second", {}) for direction in ("read", "write")]
                if not any(value.get("max", 0) > 0 for value in rates):
                    continue
                label = str((host.get("metadata") or {}).get("label") or Path(host["source"]).stem)
                cells = [cell(phase["phase"] + " / " + label + " / " + device)]
                cells += [" / ".join(fmt(value[stat] / 1024**2) if number(value.get(stat)) else "—" for stat in ("p95", "max")) for value in rates]
                flagged = {item["metric"] for item in host.get("metric_warnings", [])}
                cells += ["flagged" if prefix + key in flagged else fmt(metrics.get(prefix + key, {}).get("p95")) for key in ("await_ms", "busy_percent")]
                lines.append("| " + " | ".join(cells) + " |")
    lines += ["", "Interface rates, CPU pressure, disk queue depth and other process groups remain separate in JSON telemetry summaries."]
    if any(host.get("relevance") == "outside_phase_window" for phase in report["phases"] for host in phase["telemetry"]):
        lines += ["", "Builders with retained sample lifespans entirely outside a phase and no builds owned in it are omitted from that phase's display and coverage warnings. Their evidence remains in JSON as `outside_phase_window`. Expected owners retain missing-coverage warnings."]
    lines += ["", "## Observed evidence", ""]
    for phase in report["phases"]:
        lines.append(f"- **{cell(phase['phase'])}:** " + " ".join(phase["findings"]))
        env = phase["environment"]
        totals = {key: value["total"] if value["total"] is not None else "unreported" for key, value in env.items()}
        lines.append(f"- EROFS counters for {cell(phase['phase'])}: {totals['groups_reused']} groups reused, {totals['groups_built']} built, {totals['erofs_bytes_built']} newly built bytes. Cached multistage compile-vertex evidence: {phase['cache']['multistage_records_with_compile_hit']} records; this does not exclude concurrent executed vertices.")
    lines += ["", "## Interpretation limits", "", *["- " + value for value in LIMITATIONS], ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--phase", action="append", help="Only named immediate phase directories; repeatable")
    parser.add_argument("--telemetry", type=Path, action="append", help="Override auto-discovery; repeatable")
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    args = parser.parse_args()
    paths = sorted(args.root.glob("*/summary.json"))
    if args.phase:
        paths = [path for path in paths if path.parent.name in args.phase]
        missing = set(args.phase) - {path.parent.name for path in paths}
        if missing:
            parser.error("Missing phase summaries: " + ", ".join(sorted(missing)))
    if not paths:
        parser.error("No phase summary.json artifacts found")
    telemetry = args.telemetry if args.telemetry is not None else discover_telemetry(args.root / "telemetry")
    phases = [analyze_phase(path, telemetry) for path in paths]
    phases.sort(key=lambda phase: ((phase["window"] or {}).get("started_at", ""), str(phase["phase"])))
    report = {"schema": 1, "root": str(args.root.resolve()), "generated_at": datetime.now(timezone.utc).isoformat(),
              "latency_percentile_method": "linear interpolation", "limitations": LIMITATIONS, "phases": phases}
    json_output = args.json_output or args.root / "build-load-report.json"
    markdown_output = args.markdown_output or args.root / "build-load-report.md"
    if json_output.resolve() == markdown_output.resolve():
        parser.error("JSON and Markdown outputs must have different paths")
    for path in (json_output, markdown_output):
        if path.resolve() in {source.resolve() for source in [*paths, *telemetry]}:
            parser.error("Report output cannot overwrite an input artifact")
        path.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    markdown_output.write_text(markdown(report))
    print(json.dumps({"phases": len(phases), "json": str(json_output), "markdown": str(markdown_output)}))


if __name__ == "__main__":
    main()
