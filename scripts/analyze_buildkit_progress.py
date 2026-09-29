#!/usr/bin/env python3
"""Summarize retained plain BuildKit progress without retaining commands or output.

DONE durations measure vertices, which may overlap. Export sub-operation timings
are nested in their vertex; neither set is a reconstructed critical path. A tail
can omit vertices even when there is no explicit truncation marker.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import re
import statistics


MAX_INPUT_BYTES = 4 * 1024 * 1024
RETAINED_TAIL_CHARS = 64 * 1024
RECIPES = {"python-agent", "typescript-tools", "typescript-multistage"}
FIXTURE_ID = re.compile(r"bl\d{8}-[a-z0-9-]+-\d{3}\Z")
PROGRESS = re.compile(r"^#(\d+) (.*)$")
TERMINAL = re.compile(r"^(DONE|CACHED|CANCELED|CANCELLED|ERROR)(?:[: ](.*))?$")
SECONDS = re.compile(r"^(\d+(?:\.\d+)?)s$")
INSTRUCTION = re.compile(r"^\[([^\]]+)\] (RUN|COPY|ADD|FROM|WORKDIR|ENV|CMD|ENTRYPOINT|USER|LABEL|EXPOSE|ARG|SHELL|HEALTHCHECK|STOPSIGNAL|VOLUME|ONBUILD)(?: |$)(.*)")
OUTPUT = re.compile(r"^\d+(?:\.\d+)?(?: |$)")
DONE_SUFFIX = re.compile(r"(?: (\d+(?:\.\d+)?)s)? done$")
LAYER_TRANSFER = re.compile(r"^(sha256:[0-9a-f]{64}) (\d+(?:\.\d+)?)([kMGT]?B) / (\d+(?:\.\d+)?)([kMGT]?B)(?: |$)")
# Operands after these literal prefixes are never included in the report.
OPERATIONS = (
    ("exporting layers", "export_layers"),
    ("exporting manifest", "export_manifest"),
    ("exporting config", "export_config"),
    ("pushing layers", "push_layers"),
    ("pushing manifest", "push_manifest"),
    ("unpacking to", "unpack"),
    ("extracting sha256:", "extract_layer"),
    ("preparing build cache for export", "prepare_cache_export"),
    ("sending cache export", "send_cache_export"),
    ("writing cache image manifest", "write_cache_manifest"),
    ("writing layer", "write_cache_layer"),
    ("writing config", "write_cache_config"),
    ("transferring context:", "transfer_context"),
    ("transferring dockerfile:", "transfer_dockerfile"),
)


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "sum": 0.0, "mean": None, "median": None, "p95": None, "max": None}
    return {"count": len(ordered), "sum": sum(ordered),
            "mean": statistics.mean(ordered), "median": statistics.median(ordered),
            "p95": ordered[max(0, math.ceil(len(ordered) * .95) - 1)], "max": ordered[-1]}


def _header(text):
    """Return only fixed labels and integer step positions, never header text."""
    match = INSTRUCTION.match(text)
    if match:
        instruction, command = match[2], match[3]
        result = {"category": {"RUN": "run", "COPY": "copy", "ADD": "add", "FROM": "source"}.get(
            instruction, "other_instruction"), "instruction": instruction}
        step = re.search(r"(?:^| )(\d+)/(\d+)$", match[1])
        if step:
            result.update(step=int(step[1]), stage_steps=int(step[2]))
        if instruction == "RUN":
            result["fixture_activity"] = "other_run"
            if command.startswith("npm ci "):
                result["fixture_activity"] = "node_dependencies"
            elif command.startswith("npm run lint && npm run build && npm test"):
                result["fixture_activity"] = "node_lint_build_test_smoke"
            elif command.startswith("python -m compileall "):
                result["fixture_activity"] = "python_compile_smoke"
            elif command.startswith("python -m pip install "):
                result["fixture_activity"] = "python_native_extension" if "--no-build-isolation" in command else "python_dependencies"
        return result
    for prefix, category in (
        ("importing cache manifest from ", "cache_import"),
        ("exporting cache to ", "cache_export"),
        ("exporting to image", "image_export"),
        ("exporting to docker image format", "image_export"),
        ("exporting to oci image format", "image_export"),
        ("[internal]", "internal"),
        ("[auth]", "auth"),
    ):
        if text.startswith(prefix):
            return {"category": category}
    return None


def parse_progress(text):
    vertices = {}
    observations = Counter()
    lines = text.splitlines()
    for line in lines:
        match = PROGRESS.match(line)
        if not match:
            observations["non_progress_lines"] += 1
            continue
        vertex_id, message = int(match[1]), match[2]
        if message.startswith("building with "):
            observations["builder_banners"] += 1
            continue
        vertex = vertices.setdefault(vertex_id, {"vertex": vertex_id, "category": "unknown",
            "header_seen": False, "status": "incomplete", "duration_seconds": None,
            "terminal_observations": 0, "terminal_statuses_seen": [],
            "vertex_output_lines": 0, "layer_materialization_evidence": False,
            "operations": {}, "layer_transfers": {}})
        if OUTPUT.match(message):
            # RUN stdout is deliberately discarded, including embedded URLs/errors.
            observations["discarded_vertex_output_lines"] += 1
            vertex["vertex_output_lines"] += 1
            continue
        terminal = TERMINAL.match(message)
        if terminal:
            status = terminal[1].lower().replace("cancelled", "canceled")
            if status not in vertex["terminal_statuses_seen"]:
                vertex["terminal_statuses_seen"].append(status)
            vertex["status"] = status
            vertex["terminal_observations"] += 1
            duration = SECONDS.fullmatch(terminal[2] or "") if status == "done" else None
            vertex["duration_seconds"] = float(duration[1]) if duration else None
            continue
        header = _header(message)
        if header is not None:
            vertex.update(header, header_seen=True)
            continue
        transfer = LAYER_TRANSFER.match(message)
        if transfer:
            powers = {"B": 0, "kB": 1, "MB": 2, "GB": 3, "TB": 4}
            received = float(transfer[2]) * 1000 ** powers[transfer[3]]
            vertex["layer_transfers"][transfer[1]] = max(received, vertex["layer_transfers"].get(transfer[1], 0))
            vertex["layer_materialization_evidence"] = True
            continue
        if message.startswith("extracting sha256:"):
            vertex["layer_materialization_evidence"] = True
        done = DONE_SUFFIX.search(message)
        for prefix, operation in OPERATIONS:
            if message.startswith(prefix) and done:
                # Repeated renderings of one operation must not double-count it.
                # This in-memory key can contain operands; it is never serialized.
                key = (operation, message[:done.start()])
                vertex["operations"][key] = (operation, float(done[1]) if done[1] else None)
                break
        else:
            observations["unclassified_progress_lines"] += 1
    category_seconds = Counter()
    operation_values = defaultdict(list)
    operation_counts = Counter()
    output_vertices = []
    for vertex in vertices.values():
        vertex["reported_layer_transfer_bytes"] = sum(vertex.pop("layer_transfers").values())
        vertex["timing_category"] = vertex["category"]
        if vertex["layer_materialization_evidence"]:
            if vertex["vertex_output_lines"]:
                vertex["timing_category"] = "mixed_execution_materialization"
            else:
                vertex["timing_category"] = "cached_layer_materialization" if "cached" in vertex["terminal_statuses_seen"] else "layer_materialization"
        operations = defaultdict(list)
        untimed = Counter()
        for name, duration in vertex.pop("operations").values():
            operation_counts[name] += 1
            if duration is None:
                untimed[name] += 1
            else:
                operations[name].append(duration)
                operation_values[name].append(duration)
        vertex["operations"] = {name: {"timed_seconds": distribution(operations[name]),
            "untimed_completions": untimed[name]} for name in sorted(operations.keys() | untimed.keys())}
        if vertex["status"] == "done" and vertex["duration_seconds"] is not None:
            category_seconds[vertex["timing_category"]] += vertex["duration_seconds"]
        output_vertices.append(vertex)
    statuses = Counter(v["status"] for v in output_vertices)
    missing_headers = sum(not v["header_seen"] for v in output_vertices)
    return {"characters": len(text), "lines": len(lines),
        "coverage": {"at_or_above_known_tail_cap": len(text) >= RETAINED_TAIL_CHARS,
            "explicit_truncation_markers": text.count("[output truncated; showing retained tail]"),
            "builder_banner_seen": bool(observations["builder_banners"]),
            "first_line_is_progress": bool(lines and PROGRESS.match(lines[0])),
            "vertices_without_recognized_header": missing_headers,
            "incomplete_vertices": statuses["incomplete"],
            "complete_capture_proven": False},
        "observations": dict(observations), "status_counts": dict(statuses),
        "completed_vertex_seconds_by_category": dict(sorted(category_seconds.items())),
        "sub_operations": {name: {"timed_seconds": distribution(operation_values[name]),
            "untimed_completions": operation_counts[name] - len(operation_values[name])}
            for name in sorted(operation_counts)},
        "vertices": sorted(output_vertices, key=lambda v: v["vertex"])}


def fixture_metadata(record):
    """Project known numeric fields and fixture labels from a larger receipt."""
    image_id = record.get("image_id")
    if not isinstance(image_id, str) or not FIXTURE_ID.fullmatch(image_id):
        raise ValueError("receipt has an unrecognized fixture image ID")
    recipe = record.get("recipe")
    variant = record.get("variant")
    metadata = {"image_id": image_id, "recipe": recipe if recipe in RECIPES else "unknown",
                "variant": variant if isinstance(variant, str) and re.fullmatch(r"app-change-\d+", variant) else "unknown"}
    for name in ("index", "submission_seconds", "client_wall_seconds", "finish_offset_seconds"):
        value = record.get(name)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
            metadata[name] = value
    timings = record.get("build", {}).get("timings", {})
    phases = timings.get("phases", {})
    for name in ("docker_build_and_push_ms", "immutable_environment_ms", "cache_prepare_ms", "cache_mount_ms"):
        value = phases.get(name)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
            metadata[name.removesuffix("_ms") + "_seconds"] = value / 1000
    return metadata


def aggregate(records):
    by_recipe = defaultdict(list)
    for record in records:
        by_recipe[record["fixture"]["recipe"]].append(record)
    result = {}
    for recipe, members in sorted(by_recipe.items()):
        categories = sorted({name for r in members for name in r["progress"]["completed_vertex_seconds_by_category"]})
        operations = sorted({name for r in members for name in r["progress"]["sub_operations"]})
        activities = sorted({v["fixture_activity"] for r in members for v in r["progress"]["vertices"] if "fixture_activity" in v})
        result[recipe] = {"builds": len(members),
            "per_build_completed_vertex_seconds": {name: distribution([
                r["progress"]["completed_vertex_seconds_by_category"].get(name, 0) for r in members]) for name in categories},
            "per_build_nested_operation_seconds": {name: distribution([
                r["progress"]["sub_operations"].get(name, {}).get("timed_seconds", {}).get("sum", 0)
                for r in members]) for name in operations},
            "run_activities": {name: {
                "executed_vertex_seconds": distribution([v["duration_seconds"] for r in members for v in r["progress"]["vertices"]
                    if v.get("fixture_activity") == name and v["status"] == "done" and v["duration_seconds"] is not None and v["timing_category"] == "run"]),
                "materializing_vertex_seconds": distribution([v["duration_seconds"] for r in members for v in r["progress"]["vertices"]
                    if v.get("fixture_activity") == name and v["status"] == "done" and v["duration_seconds"] is not None and v["layer_materialization_evidence"]]),
                "vertices_with_cached_marker": sum(v.get("fixture_activity") == name and "cached" in v["terminal_statuses_seen"]
                    for r in members for v in r["progress"]["vertices"])} for name in activities},
            "per_build_reported_layer_transfer_bytes": distribution([sum(v["reported_layer_transfer_bytes"] for v in r["progress"]["vertices"]) for r in members]),
            "receipt_seconds": {name: distribution([r["fixture"][name] for r in members if name in r["fixture"]])
                for name in ("submission_seconds", "client_wall_seconds", "docker_build_and_push_seconds", "immutable_environment_seconds")}}
    return result


def analyze_directory(logs_dir, summary, *, include_vertices=True):
    rows = summary.get("records", [])
    if not isinstance(rows, list) or len(rows) > 10000:
        raise ValueError("invalid or oversized receipt list")
    records = []
    missing = []
    seen = set()
    for row in rows:
        fixture = fixture_metadata(row)
        name = fixture["image_id"]
        if name in seen:
            raise ValueError("duplicate fixture ID in receipts")
        seen.add(name)
        path = logs_dir / (name + ".build.log")
        if not path.is_file():
            missing.append(name)
            continue
        with path.open("rb") as handle:
            data = handle.read(MAX_INPUT_BYTES + 1)
        if len(data) > MAX_INPUT_BYTES:
            raise ValueError("retained log exceeds analysis input bound")
        records.append({"fixture": fixture, "input_bytes": len(data),
            "input_sha256": hashlib.sha256(data).hexdigest(),
            "progress": parse_progress(data.decode("utf-8", errors="replace"))})
    recipes = aggregate(records)
    if not include_vertices:
        for record in records:
            vertices = record["progress"].pop("vertices")
            longest = sorted((v for v in vertices if v["duration_seconds"] is not None),
                             key=lambda v: v["duration_seconds"], reverse=True)[:3]
            record["progress"]["longest_vertices"] = [{key: v[key] for key in (
                "vertex", "category", "timing_category", "status", "duration_seconds",
                "terminal_statuses_seen", "layer_materialization_evidence", "vertex_output_lines")}
                for v in longest]
    return {"schema_version": 1, "builds": len(records), "missing_logs": missing,
        "limits": [
            "DONE durations are rounded by BuildKit and overlapping vertices must not be summed as wall time or a critical path.",
            "Sub-operation timings are nested in vertex totals and can also overlap; never add them to the totals.",
            "A retained tail can omit vertices without a marker. Missing timings are unknown, not zero; per-build category sums describe observed completions only.",
            "RUN durations include all commands, scheduling and I/O in that instruction; no subcommand or CPU attribution is inferred.",
            "Repeated terminal renderings use the final observation for that vertex; numeric IDs are assumed unique within one BuildKit invocation.",
            "CACHED vertices can later emit DONE while fetching/extracting layers; these durations are materialization, not rerun commands. Mixed stdout/materialization remains explicitly ambiguous.",
            "Layer transfer bytes use rounded decimal units displayed by BuildKit, deduplicated per digest per vertex; they are not measured network traffic and may share transfers.",
        ], "recipes": recipes, "records": records}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs-dir", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--include-vertices", action="store_true", help="Include every sanitized vertex instead of only the longest three per build")
    args = parser.parse_args()
    report = analyze_directory(args.logs_dir, json.loads(args.summary.read_text()), include_vertices=args.include_vertices)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
    print(json.dumps({"builds": report["builds"], "missing_logs": len(report["missing_logs"])}))


if __name__ == "__main__":
    main()
