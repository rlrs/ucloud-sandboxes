#!/usr/bin/env python3
"""RL-scale client benchmark for plan W0/C0.1 (docs/rl-scale-architecture-plan.md).

Each live scenario drives a gateway through the public Python SDK and writes one
report with schema ``ucloud-rl-scale-bench/v1``. Every metric of the plan's
table is present in every report: the scenario's own metric carries
p50/p95/p99/max/n summaries, metrics the run did not exercise are ``not_run``,
node-side metrics (bytes fetched, idle PSS/USS, page sharing) are ``null`` with
the probe that measures them, and fork is an ``unsupported`` placeholder.
``merge`` combines single-scenario reports into one baseline report.

Every sandbox uses a unique run prefix, is tracked before its create request,
and is deleted on exit, including after failures and interrupts. Cleanup
failures are recorded and fail the run. The run refuses an occupied fleet
unless --allow-occupied-fleet is given. The gateway comes from --gateway-url or
UCLOUD_SANDBOX_URL; the sandbox API token from --api-token-file or
UCLOUD_SANDBOX_API_TOKEN. Tokens are never written to the report.

``rollout`` (plan C9.2) samples N tasks from a training selection, issues all N
creates at once, runs a per-family startup command and M agent turns with think
time, and writes the ``rollout`` metric section. Its fleet state is declared
with --fleet-state; the harness never changes the fleet, and only reads node
heartbeats (GET /v1/nodes) when given --operator-token-file.
"""

from __future__ import annotations

import argparse
import bisect
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import shlex
import signal
import statistics
import sys
import threading
import time
from typing import Any, Callable, Iterable, Sequence
import urllib.request
from urllib.parse import urlsplit
from uuid import uuid4
import zipfile


SCHEMA = "ucloud-rl-scale-bench/v1"
REPO_ROOT = Path(__file__).resolve().parents[1]
LIVE_SCENARIOS = ("cold", "warm", "burst", "rate", "density", "park", "rollout")
SCENARIOS = (*LIVE_SCENARIOS, "merged")
# The ten metrics of the plan's survey table, in table order.
METRIC_KEYS = (
    "cold_time_to_first_command",
    "warm_time_to_first_command",
    "burst_completion",
    "creation_rate",
    "bytes_fetched_share",
    "density_at_latency",
    "idle_pss_uss",
    "page_sharing_ratio",
    "pause_resume",
    "fork",
)
# Scenario metrics outside the survey table. New reports carry them (not_run
# unless measured); reports written before they existed stay valid without.
EXTRA_METRIC_KEYS = ("rollout",)
REPORT_METRIC_KEYS = (*METRIC_KEYS, *EXTRA_METRIC_KEYS)
SCENARIO_METRIC = {
    "cold": "cold_time_to_first_command",
    "warm": "warm_time_to_first_command",
    "burst": "burst_completion",
    "rate": "creation_rate",
    "density": "density_at_latency",
    "park": "pause_resume",
    "rollout": "rollout",
}
# Not observable from the SDK. Each names the probe that measures it.
EXTERNAL_METRICS = {
    "bytes_fetched_share": {
        "unit": "fraction of image bytes",
        "source": (
            "node-side chunk-cache hit/miss byte counters (plan C0.4, not yet "
            "implemented); not observable through the SDK"
        ),
    },
    "idle_pss_uss": {
        "unit": "bytes",
        "source": (
            "runtime/gvisor/spike_rl_scale.py --probe s2 (Sentry "
            "/proc/<pid>/smaps_rollup PSS and USS per sandbox)"
        ),
    },
    "page_sharing_ratio": {
        "unit": "ratio",
        "source": "runtime/gvisor/spike_rl_scale.py --probe s2 (plan C0.3)",
    },
}
FORK_REASON = "fork from a template (plan C3.3) is not implemented"
PAUSE_BYTES_SOURCE = (
    "node-side park/pause write counters (plan C0.4); not observable through the SDK"
)
SECTION_STATUSES = frozenset({"measured", "failed", "not_run", "unsupported", "external"})
REPORT_STATUSES = frozenset({"running", "ok", "failed", "interrupted"})
SUMMARY_KEYS = ("n", "p50", "p95", "p99", "max")
TRANSIENT_STATUSES = frozenset({409, 429, 502, 503, 504})
# Client-level SDK methods that would expose park/wake. SDK 0.4.x has none:
# the gateway routes need the operator control token (docs/api-reference.md).
PARK_WAKE_METHOD_PAIRS = (
    ("park_sandbox", "wake_sandbox"),
    ("pause_sandbox", "resume_sandbox"),
)
# Sandbox IDs are <run-id>-<scenario>-<index>; keep them valid node components.
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,63}")
PRINT_LOCK = threading.Lock()
_SECRETS: set[str] = set()

# -- rollout (plan C9.2) -----------------------------------------------------
SELECTORS_NAME = "all-image-selectors.json"
FLEET_STATES = ("zero", "warm-empty", "warm-seeded")
TURN_KINDS = ("grep", "test", "edit")
# Chunk-cache values that are levels, not counters: reported before/after.
ENVIRONMENT_IO_GAUGES = frozenset({
    "cached_bytes", "pending_misses", "active_components", "prefetch_enabled",
    "prefetch_jobs_active", "prefetch_ranges_inflight"})
RECIPE_GAP = (
    "the training integration builds this task from its recipe (Dockerfile and task "
    "context from its image_recipe_db); the gateway matches the recipe to this prepared "
    "foundation or source and runs the remaining build work on a builder. The selection "
    "carries no recipe, so the sandbox runs the prepared reference itself: the task "
    "delta and its live build are not exercised")
# First-command profile per family: the kind of startup work and where the
# task's working directory is expected. The image's own cwd ($PWD, under the
# linux_host profile) is tried first and a generic list last, so a wrong guess
# shows up as the workdir= marker in the samples, not as a failure.
FAMILY_PROFILES = {
    "SWE-smith": ("swe", "/testbed"),
    "R2E-Gym": ("swe", "/testbed"),
    "SWE-Lego": ("swe", "/testbed"),
    "SWE-rebench v2": ("swe", "/testbed"),
    "ScaleSWE": ("swe", "/testbed"),
    "OpenSWE": ("swe", "/testbed"),
    "MultiSWE": ("swe", "/home/*"),
    "TMax": ("terminal", "/app"),
    "Terminal-Lego": ("terminal", "/app"),
}
_PRELUDE = r'''WD=; REPO=0
for d in "$PWD" @DIRS@ /testbed /workspace /repo /app /src /home/* /root; do
  [ "$d" = / ] && continue
  if [ -d "$d/.git" ]; then WD=$d; REPO=1; break; fi
done
if [ -z "$WD" ]; then
  for d in "$PWD" @DIRS@; do [ "$d" != / ] && [ -d "$d" ] && { WD=$d; break; }; done
fi
WD=${WD:-/}; cd "$WD" || exit 90
SCAN=.; [ "$WD" = / ] && SCAN=/etc
PY=$(command -v python || command -v python3 || true)
TO=; command -v timeout >/dev/null 2>&1 && TO="timeout @TIMEOUT@"
echo "ucloud-bench workdir=$WD repo=$REPO interpreter=${PY:-none}"
'''
_PY_STARTUP = '''import os, sys
mods = []
for name in ("json", "sqlite3", "ssl", "unittest", "pytest"):
    try:
        __import__(name)
        mods.append(name)
    except Exception:
        pass
entries = 0
for path in sys.path:
    if path and os.path.isdir(path):
        entries += len(os.listdir(path))
print("ucloud-bench python=%d.%d imported=%s syspath_entries=%d" % (
    sys.version_info[0], sys.version_info[1], ",".join(mods), entries))
'''
_PY_RUN = "if [ -n \"$PY\" ]; then \"$PY\" -c '" + _PY_STARTUP + "' || exit 92; fi\n"
FIRST_COMMANDS = {
    # git status reads the index and stats every tracked file; the import
    # reads the interpreter, stdlib and site-packages.
    "swe": ('if [ "$REPO" = 1 ]; then git status --porcelain >/tmp/.ucloud-bench-status '
            '|| exit 91; echo "ucloud-bench git_status_lines=$(wc -l </tmp/.ucloud-bench-status)'
            ' tracked=$(git ls-files | wc -l)"; fi\n' + _PY_RUN),
    "terminal": ('echo "ucloud-bench workdir_entries=$(ls -A | wc -l) tree_entries='
                 '$(find "$SCAN" -maxdepth 3 2>/dev/null | head -n 5000 | wc -l)"\n' + _PY_RUN),
}
FIRST_COMMANDS["generic"] = FIRST_COMMANDS["swe"]
# The rc57 agentic turn mix (docs/image-import.md). Each turn exits 0 unless
# the sandbox itself fails; test outcomes are markers, not failures.
TURN_COMMANDS = {
    "grep": ('P="def |class |func |function "\n'
             'if [ "$REPO" = 1 ]; then n=$(git grep -n -I -E "$P" 2>/dev/null | head -n 20000 | wc -l)\n'
             'else n=$(grep -rIn -E "$P" "$SCAN" 2>/dev/null | head -n 20000 | wc -l); fi\n'
             'echo "ucloud-bench grep_lines=$n"\n'),
    "test": ('f=; [ "$REPO" = 1 ] && f=$(git ls-files 2>/dev/null | '
             'grep -E "(^|/)test_[^/]*[.]py$|_test[.]py$" | head -n 1)\n'
             'if [ -n "$f" ] && [ -n "$PY" ]; then $TO "$PY" -m pytest -q -x -p no:cacheprovider '
             '"$f" >/dev/null 2>&1; echo "ucloud-bench test=$f rc=$?"\n'
             'elif [ -n "$PY" ] && [ "$SCAN" = . ]; then $TO "$PY" -m compileall -q . '
             '>/dev/null 2>&1; echo "ucloud-bench test=compileall rc=$?"\n'
             'else echo "ucloud-bench test=none"; fi\n'),
    "edit": ('f=; [ "$REPO" = 1 ] && f=$(git ls-files 2>/dev/null | '
             'grep -E "[.](py|js|ts|go|rs|java|c|cc|cpp|h|php|rb|md|txt)$" | head -n 1)\n'
             'if [ -n "$f" ]; then printf "\\n# ucloud-bench edit\\n" >>"$f" || exit 93\n'
             '  echo "ucloud-bench edit=$f diff_bytes=$(git diff | wc -c)"\n'
             'else printf "ucloud-bench edit\\n" >>ucloud-bench-edit.txt 2>/dev/null || '
             'printf "x\\n" >>/tmp/ucloud-bench-edit.txt || exit 93\n'
             '  echo "ucloud-bench edit=file"; fi\n'),
}


# ---------------------------------------------------------------------------
# Pure computations (unit-tested without the SDK or a network).


def percentile(ordered: Sequence[float], quantile: float) -> float:
    """Nearest-rank percentile of an ascending, non-empty sequence."""
    if not ordered:
        raise ValueError("percentile of an empty sequence")
    if not 0 < quantile <= 1:
        raise ValueError("quantile must be in (0, 1]")
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)]


def latency_summary(values: Iterable[float]) -> dict[str, Any]:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return {"n": 0, "min": None, "mean": None, "p50": None, "p95": None,
                "p99": None, "max": None}
    return {
        "n": len(ordered),
        "min": round(ordered[0], 6),
        "mean": round(statistics.fmean(ordered), 6),
        "p50": round(percentile(ordered, 0.50), 6),
        "p95": round(percentile(ordered, 0.95), 6),
        "p99": round(percentile(ordered, 0.99), 6),
        "max": round(ordered[-1], 6),
    }


def assign_images(count: int, images: Sequence[str]) -> list[str]:
    """Round-robin N sandboxes over M images, so per-image counts differ by <= 1."""
    if not images:
        raise ValueError("at least one image is required")
    return [images[index % len(images)] for index in range(count)]


def duplicate_images(images: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for image in images:
        (duplicates if image in seen else seen).add(image)
    return sorted(duplicates)


def parse_command(text: str, *, name: str) -> list[str]:
    try:
        argv = shlex.split(text)
    except ValueError as exc:
        raise ValueError(f"{name} is not a valid shell word list: {exc}") from exc
    if not argv:
        raise ValueError(f"{name} is empty")
    return argv


def read_images_file(path: Path) -> list[str]:
    images = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            images.append(line)
    return images


def node_identity(response: object) -> str | None:
    """Best-effort node identity from a create response (field names vary)."""
    if not isinstance(response, dict):
        return None
    candidates: list[object] = [response.get(key) for key in ("node_id", "nodeId", "job_id")]
    record = response.get("sandbox")
    if isinstance(record, dict):
        candidates.extend(record.get(key) for key in ("node_id", "job_id"))
        for container in (record.get("node"), record.get("status")):
            if isinstance(container, dict):
                candidates.extend(container.get(key) for key in ("node_id", "job_id"))
                node = container.get("node")
                if isinstance(node, dict):
                    candidates.extend(node.get(key) for key in ("node_id", "job_id"))
    for value in candidates:
        if isinstance(value, (str, int)) and not isinstance(value, bool) and str(value):
            return str(value)
    return None


def record_id(record: object) -> str | None:
    if not isinstance(record, dict):
        return None
    spec = record.get("spec")
    if isinstance(spec, dict) and isinstance(spec.get("id"), str):
        return spec["id"]
    value = record.get("id")
    return value if isinstance(value, str) else None


def record_state(record: object) -> str | None:
    if not isinstance(record, dict):
        return None
    state = record.get("state")
    status = record.get("status")
    if not isinstance(state, str) and isinstance(status, dict):
        state = status.get("state")
    return state if isinstance(state, str) else None


def gateway_origin(url: str) -> str:
    """The gateway URL without credentials, query or fragment."""
    parts = urlsplit(url)
    host = parts.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    port = f":{parts.port}" if parts.port else ""
    return f"{parts.scheme}://{host}{port}{parts.path.rstrip('/')}"


def redact(text: str) -> str:
    for secret in _SECRETS:
        if secret:
            text = text.replace(secret, "REDACTED")
    return re.sub(r"/_relay/[^/\s'\"]+", "/_relay/REDACTED", text)


def safe_error(exc: BaseException) -> str:
    status = getattr(exc, "status_code", None)
    prefix = f"{type(exc).__name__}({status})" if isinstance(status, int) else type(exc).__name__
    return redact(f"{prefix}: {exc}")[:1200]


def not_run_section() -> dict[str, Any]:
    return {"status": "not_run"}


def external_section(metric: str) -> dict[str, Any]:
    return {"status": "external", "value": None, **EXTERNAL_METRICS[metric]}


def unsupported_section(reason: str, **extra: Any) -> dict[str, Any]:
    return {"status": "unsupported", "value": None, "reason": reason, **extra}


def default_metrics() -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for key in REPORT_METRIC_KEYS:
        if key in EXTERNAL_METRICS:
            metrics[key] = external_section(key)
        elif key == "fork":
            metrics[key] = unsupported_section(FORK_REASON)
        else:
            metrics[key] = not_run_section()
    return metrics


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_report(run_id: str, scenario: str, conditions: dict[str, Any]) -> dict[str, Any]:
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario {scenario!r}")
    return {
        "schema": SCHEMA,
        "run_id": run_id,
        "scenario": scenario,
        "status": "running",
        "ok": False,
        "started_at": utc_now(),
        "finished_at": None,
        "conditions": conditions,
        "metrics": default_metrics(),
        "errors": [],
        "cleanup": {"attempted": 0, "deleted": 0, "failed": 0,
                    "remaining_owned_ids": [], "errors": []},
        "cleanup_errors": [],
        "summary_definition": (
            "Percentiles are nearest-rank over successful samples only; failures "
            "are counted separately and never improve a percentile. Times are "
            "seconds measured on the driver with time.perf_counter()."
        ),
    }


def first_command_section(rows: Sequence[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    ok = [row for row in rows if row.get("ok")]
    return {
        "status": "measured" if ok else "failed",
        "unit": "seconds",
        "definition": "create request start through the first command's successful exit",
        "n_attempted": len(rows),
        "n_succeeded": len(ok),
        "n_failed": len(rows) - len(ok),
        "time_to_first_command": latency_summary(r["time_to_first_command_seconds"] for r in ok),
        "create": latency_summary(r["create_seconds"] for r in ok),
        "first_exec": latency_summary(r["first_exec_seconds"] for r in ok),
        **extra,
        "samples": list(rows),
    }


def burst_section(rows: Sequence[dict[str, Any]], *, images: Sequence[str],
                  concurrency: int) -> dict[str, Any]:
    ok = [row for row in rows if row.get("ok")]
    all_ready = bool(rows) and len(ok) == len(rows)
    per_image: dict[str, dict[str, int]] = {}
    nodes: dict[str, int] = {}
    for row in rows:
        entry = per_image.setdefault(row["image"], {"n": 0, "succeeded": 0})
        entry["n"] += 1
        entry["succeeded"] += int(bool(row.get("ok")))
        if row.get("ok"):
            node = row.get("node") or "unknown"
            nodes[node] = nodes.get(node, 0) + 1
    completions = [row["completion_seconds"] for row in ok]
    waits = [row["time_to_first_command_seconds"] for row in ok]
    return {
        "status": "measured" if ok else "failed",
        "unit": "seconds",
        "definition": (
            "All creates are submitted at burst start (bounded by client concurrency). "
            "completion: burst start to first command success; wait: one sandbox's "
            "own create request start to first command success."
        ),
        "sandboxes": len(rows),
        "distinct_images": len(set(images)),
        "client_concurrency": concurrency,
        "n_succeeded": len(ok),
        "n_failed": len(rows) - len(ok),
        "all_ready": all_ready,
        "all_ready_seconds": round(max(completions), 6) if all_ready else None,
        "last_ready_seconds": round(max(completions), 6) if completions else None,
        "worst_single_wait_seconds": round(max(waits), 6) if waits else None,
        "completion": latency_summary(completions),
        "wait": latency_summary(waits),
        "client_queue": latency_summary(row["queued_seconds"] for row in ok),
        "per_image": per_image,
        "nodes": nodes,
        "samples": list(rows),
    }


def rate_summary(offsets: Iterable[float], *, start: float, end: float,
                 bin_seconds: float) -> dict[str, Any]:
    """Completions per second inside [start, end], plus a per-bin timeline."""
    if bin_seconds <= 0:
        raise ValueError("bin_seconds must be positive")
    values = sorted(offsets)
    counted = [value for value in values if start <= value <= end]
    span = end - start
    horizon = max([end, *values]) if values else end
    bins = max(1, math.ceil(horizon / bin_seconds)) if horizon > 0 else 1
    timeline = [0] * bins
    for value in values:
        timeline[min(bins - 1, max(0, int(value // bin_seconds)))] += 1
    return {
        "window_start_seconds": round(start, 6),
        "window_end_seconds": round(end, 6),
        "window_seconds": round(span, 6),
        "completed_in_window": len(counted),
        "per_second": round(len(counted) / span, 6) if span > 0 else None,
        "bin_seconds": bin_seconds,
        "timeline": [{"start_seconds": round(index * bin_seconds, 6), "completed": count}
                     for index, count in enumerate(timeline)],
    }


def rate_section(rows: Sequence[dict[str, Any]], *, window_seconds: float,
                 warmup_seconds: float, bin_seconds: float, counted_event: str,
                 cap_reached_seconds: float | None, max_sandboxes: int,
                 concurrency: int) -> dict[str, Any]:
    ok = [row for row in rows if row.get("ok")]
    # A cap stops new launches early; the sustained window ends with the last
    # completion instead of crediting idle time after the cap.
    end = window_seconds
    if cap_reached_seconds is not None and ok:
        end = min(window_seconds, max(row["completion_seconds"] for row in ok))
    cluster = rate_summary((row["completion_seconds"] for row in ok),
                           start=warmup_seconds, end=end, bin_seconds=bin_seconds)
    per_node: dict[str, dict[str, Any]] = {}
    for node in sorted({row.get("node") or "unknown" for row in ok}):
        node_rows = [row["completion_seconds"] for row in ok
                     if (row.get("node") or "unknown") == node]
        summary = rate_summary(node_rows, start=warmup_seconds, end=end,
                               bin_seconds=bin_seconds)
        per_node[node] = {"completed_in_window": summary["completed_in_window"],
                          "per_second": summary["per_second"]}
    in_window_failures = [row for row in rows if not row.get("ok")
                          and warmup_seconds <= row.get("completion_seconds", -1) <= end]
    return {
        "status": "measured" if ok and cluster["per_second"] is not None else "failed",
        "unit": "creates per second",
        "counted_event": counted_event,
        "definition": (
            f"{concurrency} closed-loop client workers create until the window "
            f"ends; rate counts {counted_event} completions inside "
            "[warmup, window end] divided by that span."
        ),
        "requested_window_seconds": window_seconds,
        "warmup_seconds": warmup_seconds,
        "max_sandboxes": max_sandboxes,
        "cap_reached_seconds": (round(cap_reached_seconds, 6)
                                if cap_reached_seconds is not None else None),
        "n_attempted": len(rows),
        "n_succeeded": len(ok),
        "n_failed": len(rows) - len(ok),
        "n_failed_in_window": len(in_window_failures),
        "completed_after_window": sum(row["completion_seconds"] > end for row in ok),
        "cluster": cluster,
        "per_node": per_node,
        "create_latency": latency_summary(row["create_seconds"] for row in ok),
        "samples": list(rows),
    }


def bin_by_resident(samples: Sequence[dict[str, Any]], bin_width: int) -> list[dict[str, Any]]:
    """Group tool-call samples into resident-count bins [low, high] of bin_width."""
    if bin_width <= 0:
        raise ValueError("bin_width must be positive")
    groups: dict[int, list[dict[str, Any]]] = {}
    for sample in samples:
        resident = int(sample["resident"])
        if resident <= 0:
            raise ValueError("resident count must be positive")
        groups.setdefault((resident - 1) // bin_width, []).append(sample)
    bins = []
    for key in sorted(groups):
        rows = groups[key]
        ok = [row["seconds"] for row in rows if row.get("ok")]
        bins.append({
            "resident_low": key * bin_width + 1,
            "resident_high": (key + 1) * bin_width,
            "resident_max_observed": max(int(row["resident"]) for row in rows),
            "n_attempted": len(rows),
            "n_failed": len(rows) - len(ok),
            "latency": latency_summary(ok),
        })
    return bins


def density_at_limit(bins: Sequence[dict[str, Any]], limit_seconds: float) -> dict[str, Any]:
    """Largest resident count before the first bin that misses the p99 limit.

    A bin with failed calls or no successful sample counts as a violation:
    successful-call percentiles never make a partial bin pass.
    """
    best = None
    violation = None
    for row in sorted(bins, key=lambda item: item["resident_low"]):
        p99 = row["latency"]["p99"]
        if row["n_failed"] or p99 is None or p99 > limit_seconds:
            violation = row
            break
        best = row["resident_max_observed"]
    return {
        "p99_limit_seconds": limit_seconds,
        "max_resident_within_limit": best,
        "first_violating_bin": (
            None if violation is None else {
                key: violation[key] for key in (
                    "resident_low", "resident_high", "resident_max_observed",
                    "n_attempted", "n_failed")
            } | {"p99": violation["latency"]["p99"]}
        ),
    }


def density_section(samples: Sequence[dict[str, Any]], *, bin_width: int,
                    limit_seconds: float, tool_command: Sequence[str],
                    steps: Sequence[dict[str, Any]], stop_reason: str | None) -> dict[str, Any]:
    bins = bin_by_resident(samples, bin_width) if samples else []
    verdict = density_at_limit(bins, limit_seconds)
    create_failures = sum(step.get("create_failures", 0) for step in steps)
    return {
        "status": "measured" if bins else "failed",
        "n_failed": create_failures + sum(not sample.get("ok") for sample in samples),
        "n_failed_creates": create_failures,
        "unit": "seconds",
        "definition": (
            "Resident sandboxes ramp in steps; at each step a fixed number of "
            "tool calls run round-robin over all residents. Latency is exec "
            "start through successful exit."
        ),
        "tool_command": list(tool_command),
        "bin_width": bin_width,
        **verdict,
        "stop_reason": stop_reason,
        "bins": bins,
        "steps": list(steps),
        "samples": list(samples),
    }


def sdk_park_wake_methods(client: object) -> tuple[str, str] | None:
    for park, wake in PARK_WAKE_METHOD_PAIRS:
        if callable(getattr(client, park, None)) and callable(getattr(client, wake, None)):
            return park, wake
    return None


def load_selection(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Entries and provenance from a selection archive, its unpacked directory,
    its all-image-selectors.json, or a plain images file (REFERENCE or
    FAMILY<TAB>REFERENCE per line, # comments)."""
    member = None
    if path.is_file() and zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            names = sorted((name for name in archive.namelist()
                            if name.rsplit("/", 1)[-1] == SELECTORS_NAME), key=len)
            if not names:
                raise ValueError(f"{path} contains no {SELECTORS_NAME}")
            member, data = names[0], archive.read(names[0])
    elif path.is_dir():
        found = next((candidate for candidate in
                      [path / SELECTORS_NAME, *sorted(path.glob(f"*/{SELECTORS_NAME}"))]
                      if candidate.is_file()), None)
        if found is None:
            raise ValueError(f"{path} contains no {SELECTORS_NAME}")
        member, data = str(found.relative_to(path)), found.read_bytes()
    else:
        data = path.read_bytes()
    text = data.decode("utf-8")
    provenance: dict[str, Any] = {"path": str(path), "member": member,
                                  "sha256": hashlib.sha256(data).hexdigest()}
    if text.lstrip().startswith("{"):
        document = json.loads(text)
        entries = document.get("images") if isinstance(document, dict) else None
        if not isinstance(entries, list):
            raise ValueError(f"{path}: selection JSON has no images[] array")
        provenance.update(format="selectors", **{key: document.get(key) for key in (
            "schema", "observed_at_utc", "scope", "environment_revision", "verifiers_revision")})
    else:
        entries = []
        for line in text.splitlines():
            if line.strip() and not line.strip().startswith("#"):
                family, tab, image = line.strip().rpartition("\t")
                entries.append({"family": family.strip() if tab else "unknown",
                                "image": image.strip()})
        provenance["format"] = "images-file"
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict) or not all(
                isinstance(entry.get(key), str) and entry[key] for key in ("family", "image")):
            raise ValueError(f"{path}: entry {index} needs non-empty family and image")
        rows = entry.setdefault("upstream_rows", 1)
        if type(rows) is not int or rows <= 0:
            raise ValueError(f"{path}: entry {index} upstream_rows must be a positive integer")
    if not entries:
        raise ValueError(f"{path}: the selection is empty")
    provenance.update(entries=len(entries),
                      upstream_rows=sum(entry["upstream_rows"] for entry in entries))
    return entries, provenance


def sample_tasks(entries: Sequence[dict[str, Any]], *, count: int, seed: int,
                 weighting: str, families: Sequence[str] = ()) -> list[dict[str, Any]]:
    """Deterministic task sample. ``rows`` draws task rows without replacement,
    as training does, so an image recurs once per sampled row that uses it;
    ``uniform`` draws distinct images."""
    wanted = {family.casefold() for family in families}
    pool = [entry for entry in entries if not wanted or entry["family"].casefold() in wanted]
    if not pool:
        raise ValueError("no selection entry matches --family " + ", ".join(families))
    rng = random.Random(seed)
    if weighting == "uniform":
        if count > len(pool):
            raise ValueError(f"--tasks {count} exceeds the {len(pool)} images in the pool")
        return rng.sample(pool, count)
    cumulative = list(itertools.accumulate(entry["upstream_rows"] for entry in pool))
    if count > cumulative[-1]:
        raise ValueError(f"--tasks {count} exceeds the {cumulative[-1]} task rows in the pool")
    chosen = [pool[bisect.bisect_right(cumulative, row)]
              for row in rng.sample(range(cumulative[-1]), count)]
    return chosen


def resolve_image(entry: dict[str, Any], source: str) -> dict[str, Any]:
    """The reference a sandbox names, and how close it is to the training path."""
    prepared = entry.get("prepared_reference")
    if "cached_kind" not in entry and not prepared:
        return {"reference": entry["image"], "resolution": "as_listed"}
    attach_only = not entry.get("remaining_build_work") and entry.get("task_source_cached") is True
    if source == "auto" and attach_only and entry.get("cached_kind") == "source":
        # Source preparation registered the upstream reference as an import
        # alias of the prepared digest: the gateway resolves it on create.
        return {"reference": entry["image"], "resolution": "import_alias"}
    if not isinstance(prepared, str) or not prepared:
        raise ValueError(f"{entry['image']}: no prepared_reference")
    if attach_only:
        return {"reference": prepared, "resolution": "prepared_reference"}
    return {"reference": prepared, "resolution": "prepared_reference_fallback",
            "remaining_build_work": list(entry.get("remaining_build_work") or [])}


def parse_assignments(items: Sequence[str], *, name: str,
                      keys: Sequence[str] | None = None) -> dict[str, str]:
    result = {}
    for item in items:
        key, separator, value = item.partition("=")
        if not separator or not key.strip() or not value.strip():
            raise ValueError(f"{name} must be KEY=SHELL: {item!r}")
        if keys is not None and key.strip() not in keys:
            raise ValueError(f"{name} key must be one of {', '.join(keys)}: {key!r}")
        result[key.strip()] = value
    return result


def shell_script(dirs: str, body: str, *, timeout_seconds: float) -> list[str]:
    prelude = _PRELUDE.replace("@DIRS@", dirs).replace(
        "@TIMEOUT@", str(max(1, int(timeout_seconds) - 5)))
    return ["sh", "-c", prelude + body]


def rollout_commands(families: Iterable[str], *, family_overrides: dict[str, str],
                     turn_overrides: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
    """argv per family (first command) and per turn kind, prelude included."""
    first = {}
    for family in sorted(set(families)):
        kind, dirs = FAMILY_PROFILES.get(family, ("generic", ""))
        body = family_overrides.get(family, family_overrides.get("*", FIRST_COMMANDS[kind]))
        first[family] = shell_script(dirs, body, timeout_seconds=timeout_seconds)
    turns = {kind: shell_script("", turn_overrides.get(kind, TURN_COMMANDS[kind]),
                                timeout_seconds=timeout_seconds) for kind in TURN_KINDS}
    return {"first": first, "turns": turns}


def parse_markers(stdout: str) -> dict[str, str]:
    """key=value tokens from the commands' ``ucloud-bench`` lines."""
    markers: dict[str, str] = {}
    for line in stdout.splitlines():
        if line.startswith("ucloud-bench "):
            for token in line.split()[1:]:
                key, separator, value = token.partition("=")
                if separator and len(markers) < 24:
                    markers[key[:64]] = value[:200]
    return markers


def error_code_of(exc: BaseException) -> str:
    code = getattr(exc, "error_code", None)
    body = getattr(exc, "body", None)
    if not code and isinstance(body, dict):
        code = body.get("error_code")
    if isinstance(code, str) and code:
        return code
    status = getattr(exc, "status_code", None)
    return f"http_{status}" if isinstance(status, int) else type(exc).__name__


def create_phases(timings: object) -> dict[str, float] | None:
    """C0.2 phase timings from a create response: the worker's own phases and
    the sandbox manager's (``timings.manager.phases``), in milliseconds."""
    if not isinstance(timings, dict):
        return None
    phases: dict[str, float] = {}
    manager = timings.get("manager")
    for source in (timings.get("phases"), manager.get("phases") if isinstance(manager, dict) else None):
        if isinstance(source, dict):
            phases.update({str(key): float(value) for key, value in source.items()
                           if type(value) in (int, float) and math.isfinite(value)})
    return phases or None


def _timestamp(value: object) -> float | None:
    try:
        return datetime.fromisoformat(str(value)).timestamp() if value else None
    except ValueError:
        return None


def fleet_snapshot(nodes: Sequence[dict[str, Any]], *, now: float,
                   fresh_seconds: float) -> dict[str, Any]:
    """Sandbox-capable nodes from GET /v1/nodes; fresh = heartbeat received
    within fresh_seconds (the gateway's default heartbeat TTL is 120 s)."""
    rows = []
    for node in nodes:
        if not isinstance(node, dict) or "sandbox" not in (node.get("capabilities") or []):
            continue
        seen = _timestamp(node.get("received_at")) or _timestamp(node.get("updated_at"))
        age = None if seen is None else round(now - seen, 3)
        metrics = node.get("runtime_metrics") if isinstance(node.get("runtime_metrics"), dict) else {}
        rows.append({"node_id": node.get("node_id"), "job_id": node.get("job_id"),
                     "node_epoch": node.get("node_epoch"), "age_seconds": age,
                     "fresh": age is not None and age <= fresh_seconds,
                     "active_sandboxes": node.get("active_sandboxes"),
                     "environment_io": metrics.get("environment_io")})
    fresh = [row for row in rows if row["fresh"]]
    return {"observed_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "fresh_seconds": fresh_seconds, "sandbox_nodes_stored": len(rows),
            "sandbox_nodes_fresh": len(fresh),
            "cached_bytes_fresh": sum((row["environment_io"] or {}).get("cached_bytes", 0)
                                      for row in fresh), "nodes": rows}


def environment_io_deltas(before: dict[str, Any] | None,
                          after: dict[str, Any]) -> dict[str, Any]:
    """Per-node counter deltas between two snapshots. A node that is new or
    restarted (another node_epoch, or a counter went down) counts from zero."""
    old = {row["node_id"]: row for row in (before or {}).get("nodes", [])}
    per_node: dict[str, Any] = {}
    totals: dict[str, float] = {}
    for row in after["nodes"]:
        current = row.get("environment_io")
        if not isinstance(current, dict):
            continue
        previous = old.get(row["node_id"]) or {}
        baseline = previous.get("environment_io") if previous.get(
            "node_epoch") == row.get("node_epoch") else None
        counters = {key: value for key, value in current.items()
                    if key not in ENVIRONMENT_IO_GAUGES and type(value) in (int, float)}
        reset = not isinstance(baseline, dict) or any(
            value < baseline.get(key, 0) for key, value in counters.items())
        deltas = {key: value - (0 if reset else baseline.get(key, 0))
                  for key, value in counters.items()}
        for key, value in deltas.items():
            totals[key] = totals.get(key, 0) + value
        per_node[str(row["node_id"])] = {
            "counted_from_zero": reset, "deltas": deltas,
            "gauges": {key: {"before": None if reset else baseline.get(key),
                             "after": current.get(key)}
                       for key in sorted(ENVIRONMENT_IO_GAUGES) if key in current}}
    lookups = totals.get("hits", 0) + totals.get("misses", 0)
    return {"totals": totals, "per_node": per_node,
            "hit_ratio": round(totals.get("hits", 0) / lookups, 6) if lookups else None}


def rollout_section(rows: Sequence[dict[str, Any]], *, tasks: int, fleet_state: str,
                    arrival: dict[str, Any], node_io: dict[str, Any],
                    slowest: int = 20) -> dict[str, Any]:
    ok = [row for row in rows if row.get("ok")]

    def steps(group: Sequence[dict[str, Any]]) -> dict[str, Any]:
        return {"n": len(group), "n_failed": sum(not row.get("ok") for row in group),
                "time_to_ready": latency_summary(row["time_to_ready_seconds"] for row in group
                                                 if "time_to_ready_seconds" in row),
                "time_to_first_command": latency_summary(
                    row["time_to_first_command_seconds"] for row in group
                    if row.get("first_command_ok"))}

    families = sorted({row["family"] for row in rows})
    phases: dict[str, list[float]] = {}
    failures: dict[str, dict[str, Any]] = {}
    turns: dict[str, dict[str, list[float]]] = {}
    turn_failures: dict[str, int] = {}
    resolutions: dict[str, int] = {}
    for row in rows:
        resolutions[row["image_resolution"]] = resolutions.get(row["image_resolution"], 0) + 1
        for name, value in (row.get("create_phases_ms") or {}).items():
            phases.setdefault(name, []).append(value)
        if not row.get("ok"):
            entry = failures.setdefault(row.get("error_code") or "unknown",
                                        {"n": 0, "by_phase": {}, "example": row.get("error")})
            entry["n"] += 1
            entry["by_phase"][row["phase"]] = entry["by_phase"].get(row["phase"], 0) + 1
        seen: set[str] = set()
        for turn in row.get("turns", []):
            if not turn["ok"]:
                turn_failures[turn["kind"]] = turn_failures.get(turn["kind"], 0) + 1
                continue
            bucket = turns.setdefault(turn["kind"], {"all": [], "first": [], "later": []})
            bucket["all"].append(turn["seconds"])
            bucket["later" if turn["kind"] in seen else "first"].append(turn["seconds"])
            seen.add(turn["kind"])
    ranked = sorted((row for row in rows if row.get("first_command_ok")),
                    key=lambda row: row["time_to_first_command_seconds"], reverse=True)
    distinct = {row["reference"]: row.get("erofs_bytes") for row in rows}
    image_bytes = sum(value for value in distinct.values() if isinstance(value, int))
    if node_io.get("status") == "measured" and image_bytes:
        node_io["bytes_fetched_share_estimate"] = {
            "value": float(f"{node_io['environment_io']['totals'].get('downloaded_bytes', 0) / image_bytes:.6g}"),
            "definition": "chunk bytes downloaded by all nodes during the run / EROFS bytes "
                          "of the distinct references started (selection erofs_bytes)"}
    return {
        "status": "measured" if ok else "failed",
        "unit": "seconds",
        "definition": (
            "Every sandbox's create is issued at its arrival offset (all at once unless "
            "--ramp-seconds), then its family's first command and M turns with think time; "
            "it is deleted when its turns end. time_to_ready: create request start to create "
            "returned; time_to_first_command: create request start to first command exit. "
            "Each summary counts the samples that completed that step."),
        "fleet_state": fleet_state,
        "tasks": tasks,
        "n_started": len(rows),
        "n_not_started": tasks - len(rows),
        "n_succeeded": len(ok),
        "n_failed": len(rows) - len(ok),
        "arrival": arrival,
        "images": {"distinct_references": len(distinct), "by_resolution": resolutions,
                   "erofs_bytes_distinct": image_bytes or None},
        "overall": steps(rows),
        "per_family": {family: steps([row for row in rows if row["family"] == family])
                       for family in families},
        "first_command": latency_summary(row["first_command_seconds"] for row in rows
                                         if row.get("first_command_ok")),
        "slowest": [{key: row.get(key) for key in (
            "sandbox_id", "family", "image", "reference", "image_resolution", "node",
            "time_to_ready_seconds", "time_to_first_command_seconds")}
            for row in ranked[:slowest]],
        "create_phases_ms": {
            "source": "create response timings.phases and timings.manager.phases (plan C0.2)",
            "n_with_phases": sum(bool(row.get("create_phases_ms")) for row in rows),
            "phases": {name: latency_summary(values) for name, values in sorted(phases.items())}},
        "failures_by_error_code": failures,
        "turns": {"n": sum(len(row.get("turns", [])) for row in rows),
                  "n_failed": sum(turn_failures.values()),
                  "by_kind": {kind: {"n_failed": turn_failures.get(kind, 0),
                                     **{name: latency_summary(values) for name, values in
                                        turns.get(kind, {"all": [], "first": [], "later": []}).items()}}
                              for kind in sorted(set(turns) | set(turn_failures))}},
        "node_io": node_io,
        "samples": list(rows),
    }


def _summary_problems(value: dict[str, Any], path: str) -> list[str]:
    problems = []
    n = value.get("n")
    if type(n) is not int or n < 0:
        return [f"{path}.n must be a non-negative integer"]
    stats = [value.get(key) for key in SUMMARY_KEYS[1:]]
    if n == 0:
        if any(item is not None for item in stats):
            problems.append(f"{path}: an empty summary must have null percentiles")
        return problems
    if any(type(item) not in (int, float) or not math.isfinite(item) for item in stats):
        return [f"{path}: percentiles must be finite numbers when n > 0"]
    if any(left > right for left, right in zip(stats, stats[1:])):
        problems.append(f"{path}: require p50 <= p95 <= p99 <= max")
    return problems


def _walk_summaries(value: object, path: str) -> Iterable[tuple[str, dict[str, Any]]]:
    if isinstance(value, dict):
        if all(key in value for key in SUMMARY_KEYS):
            yield path, value
        for key, item in value.items():
            if key != "samples":
                yield from _walk_summaries(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk_summaries(item, f"{path}[{index}]")


def validate_report(report: object) -> list[str]:
    """Return schema problems; an empty list means the report is valid."""
    if not isinstance(report, dict):
        return ["report must be a JSON object"]
    problems = []
    if report.get("schema") != SCHEMA:
        problems.append(f"schema must be {SCHEMA!r}")
    for key in ("run_id", "started_at"):
        if not isinstance(report.get(key), str) or not report.get(key):
            problems.append(f"{key} must be a non-empty string")
    if report.get("scenario") not in SCENARIOS:
        problems.append(f"scenario must be one of {', '.join(SCENARIOS)}")
    if report.get("status") not in REPORT_STATUSES:
        problems.append(f"status must be one of {', '.join(sorted(REPORT_STATUSES))}")
    if type(report.get("ok")) is not bool:
        problems.append("ok must be a boolean")
    if not isinstance(report.get("conditions"), dict):
        problems.append("conditions must be an object")
    for key in ("errors", "cleanup_errors"):
        if not isinstance(report.get(key), list):
            problems.append(f"{key} must be a list")
    if not isinstance(report.get("cleanup"), dict):
        problems.append("cleanup must be an object")
    metrics = report.get("metrics")
    if not isinstance(metrics, dict):
        return [*problems, "metrics must be an object"]
    missing = [key for key in METRIC_KEYS if key not in metrics]
    unknown = sorted(set(metrics) - set(REPORT_METRIC_KEYS))
    if missing:
        problems.append("metrics missing: " + ", ".join(missing))
    if unknown:
        problems.append("unknown metrics: " + ", ".join(unknown))
    for key in REPORT_METRIC_KEYS:
        section = metrics.get(key)
        if section is None:
            continue
        if not isinstance(section, dict) or section.get("status") not in SECTION_STATUSES:
            problems.append(f"metrics.{key}.status must be one of "
                            + ", ".join(sorted(SECTION_STATUSES)))
            continue
        if key in EXTERNAL_METRICS:
            if section.get("status") != "external" or section.get("value") is not None:
                problems.append(f"metrics.{key} is node-side: status external, value null")
            if not isinstance(section.get("source"), str) or not section["source"]:
                problems.append(f"metrics.{key}.source must name the measuring probe")
        elif section.get("status") == "external":
            problems.append(f"metrics.{key} is client-measured and cannot be external")
        if key == "fork" and (section.get("status") != "unsupported"
                              or section.get("value") is not None):
            problems.append("metrics.fork must remain an unsupported placeholder")
        if section.get("status") == "unsupported" and not section.get("reason"):
            problems.append(f"metrics.{key}: unsupported sections need a reason")
        for path, summary in _walk_summaries(section, f"metrics.{key}"):
            problems.extend(_summary_problems(summary, path))
    return problems


def finalize_report(report: dict[str, Any], *, interrupted: bool = False) -> dict[str, Any]:
    report["finished_at"] = utc_now()
    scenario = report.get("scenario")
    sections = ([report["metrics"][SCENARIO_METRIC[scenario]]]
                if scenario in SCENARIO_METRIC else list(report["metrics"].values()))
    # Any failed sandbox or call fails the run; its samples stay in the report.
    failed = any(section["status"] == "failed" or section.get("n_failed")
                 for section in sections)
    if scenario in SCENARIO_METRIC and sections[0]["status"] == "not_run":
        failed = True
    report["ok"] = not (interrupted or failed or report["errors"] or report["cleanup_errors"])
    report["status"] = "interrupted" if interrupted else ("ok" if report["ok"] else "failed")
    return report


def merge_reports(reports: Sequence[tuple[str, dict[str, Any]]]) -> dict[str, Any]:
    """Combine single-scenario reports; each metric may come from one input only."""
    if not reports:
        raise ValueError("merge needs at least one report")
    for source, report in reports:
        problems = validate_report(report)
        if problems:
            raise ValueError(f"{source} is not a valid {SCHEMA} report: " + "; ".join(problems))
    merged = new_report("rlbench-merged-" + uuid4().hex[:12], "merged", {
        "merged_from": [
            {"source": source, "run_id": report["run_id"], "scenario": report["scenario"],
             "status": report["status"], "started_at": report["started_at"],
             "finished_at": report.get("finished_at"), "conditions": report["conditions"]}
            for source, report in reports
        ],
    })
    merged["started_at"] = min(report["started_at"] for _, report in reports)
    for key in REPORT_METRIC_KEYS:
        if key in EXTERNAL_METRICS or key == "fork":
            continue
        found = [(source, report["metrics"][key]) for source, report in reports
                 if report["metrics"].get(key, {"status": "not_run"})["status"] != "not_run"]
        if len(found) > 1:
            raise ValueError(f"metric {key} is reported by several inputs: "
                             + ", ".join(source for source, _ in found))
        if found:
            merged["metrics"][key] = {**found[0][1], "merged_from": found[0][0]}
    for source, report in reports:
        merged["errors"].extend(f"{source}: {error}" for error in report["errors"])
        merged["cleanup_errors"].extend(f"{source}: {error}" for error in report["cleanup_errors"])
        if not report["ok"]:
            merged["errors"].append(f"{source}: input run status is {report['status']}")
    finalize_report(merged)
    finished = [report.get("finished_at") for _, report in reports if report.get("finished_at")]
    merged["finished_at"] = max(finished) if finished else merged["finished_at"]
    return merged


# ---------------------------------------------------------------------------
# Report persistence and argument parsing.


def emit(event: str, **fields: Any) -> None:
    with PRINT_LOCK:
        print(json.dumps({"event": event, "at": utc_now(), **fields}, sort_keys=True,
                         default=str), flush=True)


def write_report(path: Path, report: dict[str, Any]) -> None:
    """Atomically replace the report so a crash never leaves a torn file."""
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
                         encoding="utf-8")
    temporary.replace(path)


def reserve_output(path: Path, *, overwrite: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if overwrite:
        return
    # Never overwrite another run's evidence.
    with path.open("x", encoding="utf-8"):
        pass


def _positive_int(text: str) -> int:
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def _positive_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be a finite positive number")
    return value


def _non_negative_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError("must be a finite non-negative number")
    return value


def _non_negative_int(text: str) -> int:
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return value


def _seconds_range(text: str) -> tuple[float, float]:
    low, _, high = text.partition(":")
    try:
        bounds = (_non_negative_float(low), _non_negative_float(high or low))
    except (ValueError, argparse.ArgumentTypeError) as exc:
        raise argparse.ArgumentTypeError(f"must be LO:HI seconds: {exc}") from exc
    if bounds[0] > bounds[1]:
        raise argparse.ArgumentTypeError("LO must not exceed HI")
    return bounds


def _live_parent() -> argparse.ArgumentParser:
    parent = argparse.ArgumentParser(add_help=False)
    group = parent.add_argument_group("gateway and report")
    group.add_argument("--gateway-url", default=os.environ.get("UCLOUD_SANDBOX_URL"),
                       help="gateway base URL (default: $UCLOUD_SANDBOX_URL)")
    group.add_argument("--api-token-file", type=Path,
                       help="sandbox API token file (default: $UCLOUD_SANDBOX_API_TOKEN)")
    group.add_argument("--output", type=Path, required=True, help="JSON report path")
    group.add_argument("--overwrite", action="store_true",
                       help="replace an existing report instead of refusing")
    group.add_argument("--run-id", help="sandbox ID prefix (default: rlbench-<random>)")
    group.add_argument("--allow-occupied-fleet", action="store_true",
                       help="measure even when other sandboxes are visible")
    group.add_argument("--request-timeout-seconds", type=_positive_float, default=120.0)
    group.add_argument("--create-timeout-seconds", type=_positive_float, default=900.0)
    group.add_argument("--exec-timeout-seconds", type=_positive_float, default=120.0)
    group.add_argument("--cleanup-timeout-seconds", type=_positive_float, default=300.0)
    shape = parent.add_argument_group("sandbox shape")
    shape.add_argument("--cpus", type=_positive_float, default=1.0)
    shape.add_argument("--memory-mb", type=_positive_int, default=1024)
    shape.add_argument("--disk-mb", type=_positive_int, default=2048)
    shape.add_argument("--ttl-seconds", type=_positive_int, default=1800)
    shape.add_argument("--network", choices=("bridge", "none"), default="bridge")
    shape.add_argument("--image-kind", choices=("registry", "name"), default="registry",
                       help="Image.from_registry (default) or Image.from_name")
    shape.add_argument("--sandbox-command",
                       help="keep-alive command (default: 'sleep <ttl>', rollout: image "
                            "default; '' = image default)")
    shape.add_argument("--profile", choices=("container", "linux_host"),
                       help="sandbox profile (default: container; rollout: linux_host, "
                            "as SandboxSpec.benchmark and the training integration use)")
    shape.add_argument("--first-command",
                       help="command whose first success marks a sandbox ready (default: "
                            "true; rollout uses its per-family table, see --family-command)")
    shape.add_argument("--label", action="append", default=[], metavar="KEY=VALUE",
                       help="extra sandbox label; repeatable")
    return parent


def _add_images(parser: argparse.ArgumentParser, *, help_text: str) -> None:
    parser.add_argument("--image", action="append", default=[], help=help_text)
    parser.add_argument("--images-file", type=Path,
                        help="file with one image reference per line (# comments)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="scenario", metavar="SCENARIO", required=True)
    live = _live_parent()

    cold = commands.add_parser(
        "cold", parents=[live], help="cold time to first command (caller-asserted cold images)",
        description="Cold time to first command. The caller asserts every image was "
                    "never pulled by any node; the harness cannot verify it.")
    _add_images(cold, help_text="image the caller asserts is cold; repeatable, no duplicates")
    cold.add_argument("--concurrency", type=_positive_int, default=1)

    warm = commands.add_parser("warm", parents=[live], help="warm time to first command",
                               description="Warm time to first command: one image repeated.")
    warm.add_argument("--image", required=True)
    warm.add_argument("--repeats", type=_positive_int, default=10)
    warm.add_argument("--no-prime", action="store_true",
                      help="do not run an unmeasured priming create first")

    burst = commands.add_parser("burst", parents=[live], help="burst completion",
                                description="N sandboxes over M images with bounded concurrency.")
    _add_images(burst, help_text="image; repeatable (sandboxes are assigned round-robin)")
    burst.add_argument("--sandboxes", type=_positive_int, default=64)
    burst.add_argument("--concurrency", type=_positive_int, default=32)

    rate = commands.add_parser("rate", parents=[live], help="sustained creation rate",
                               description="Closed-loop creates for a fixed window.")
    _add_images(rate, help_text="image; repeatable (round-robin)")
    rate.add_argument("--window-seconds", type=_positive_float, default=60.0)
    rate.add_argument("--warmup-seconds", type=_non_negative_float, default=5.0)
    rate.add_argument("--bin-seconds", type=_positive_float, default=1.0)
    rate.add_argument("--concurrency", type=_positive_int, default=32)
    rate.add_argument("--max-sandboxes", type=_positive_int, default=512,
                      help="stop launching after this many creates")
    rate.add_argument("--ready", action="store_true",
                      help="count first-command readiness instead of create completion")

    density = commands.add_parser("density", parents=[live], help="density at fixed p99 latency",
                                  description="Ramp residents; p99 tool latency per bin.")
    density.add_argument("--image", required=True)
    density.add_argument("--step", type=_positive_int, default=16)
    density.add_argument("--max-resident", type=_positive_int, default=256)
    density.add_argument("--create-concurrency", type=_positive_int, default=16)
    density.add_argument("--probes-per-bin", type=_positive_int, default=64)
    density.add_argument("--probe-concurrency", type=_positive_int, default=16)
    density.add_argument("--tool-command", default="sh -c true",
                         help="small fixed tool call, e.g. \"python3 -c 'print(1)'\"")
    density.add_argument("--p99-limit-seconds", type=_positive_float, default=1.0)
    density.add_argument("--continue-past-limit", action="store_true",
                         help="keep ramping after the first violating bin")

    park = commands.add_parser("park", parents=[live], help="pause/park and resume/wake latency",
                               description="Uses SDK park/wake methods if the SDK exposes "
                                           "them; otherwise records the metric as unsupported.")
    park.add_argument("--image", required=True)
    park.add_argument("--cycles", type=_positive_int, default=5)

    rollout = commands.add_parser(
        "rollout", parents=[live], help="realistic RL rollout start (plan C9.2)",
        description="N tasks sampled from a training selection, all created at once, "
                    "each running its family's startup command and M agent turns.")
    rollout.add_argument("--selection", type=Path, required=True,
                         help="selection .zip, its unpacked directory, all-image-selectors.json, "
                              "or a plain images file (REFERENCE or FAMILY<TAB>REFERENCE)")
    rollout.add_argument("--tasks", type=_positive_int, default=500)
    rollout.add_argument("--seed", type=int, default=0)
    rollout.add_argument("--sampling", choices=("rows", "uniform"), default="rows",
                         help="rows: weighted by upstream_rows, like training (default); "
                              "uniform: distinct images")
    rollout.add_argument("--family", action="append", default=[],
                         help="only sample this family; repeatable")
    rollout.add_argument("--image-source", choices=("auto", "prepared"), default="auto",
                         help="auto: what the integration resolves to (import aliases for "
                              "attach-only sources); prepared: always prepared_reference")
    rollout.add_argument("--ramp-seconds", type=_non_negative_float, default=0.0,
                         help="spread arrivals evenly over this window (default: all at once)")
    rollout.add_argument("--turns", type=_non_negative_int, default=8)
    rollout.add_argument("--think-seconds", type=_seconds_range, default=(5.0, 30.0),
                         metavar="LO:HI", help="uniform think time before each turn")
    rollout.add_argument("--turn-mix", default=",".join(TURN_KINDS),
                         help="turn kinds rotated per sandbox (default: grep,test,edit)")
    rollout.add_argument("--family-command", action="append", default=[],
                         metavar="FAMILY=SHELL",
                         help="replace a family's first command ('*' = every family); "
                              "runs after the workdir prelude ($WD, $REPO, $PY, $TO)")
    rollout.add_argument("--turn-command", action="append", default=[], metavar="KIND=SHELL",
                         help="replace a turn kind's command (grep, test, edit)")
    rollout.add_argument("--fleet-state", choices=FLEET_STATES, required=True,
                         help="declared fleet and cache state; recorded, never changed")
    rollout.add_argument("--parkable", action="store_true",
                         help="create parkable sandboxes, so think time can park them")
    rollout.add_argument("--operator-token-file", type=Path,
                         help="gateway control token: read GET /v1/nodes to verify "
                              "--fleet-state zero and record heartbeat environment_io")
    rollout.add_argument("--node-fresh-seconds", type=_positive_float, default=120.0)
    rollout.add_argument("--node-poll-seconds", type=_non_negative_float, default=15.0,
                         help="node-count timeline interval with an operator token (0 = off)")
    rollout.add_argument("--heartbeat-settle-seconds", type=_non_negative_float, default=20.0,
                         help="wait before the final heartbeat read")

    validate = commands.add_parser("validate", help="validate report files against the schema")
    validate.add_argument("reports", nargs="+", type=Path)

    merge = commands.add_parser("merge", help="combine single-scenario reports")
    merge.add_argument("--output", type=Path, required=True)
    merge.add_argument("--overwrite", action="store_true")
    merge.add_argument("reports", nargs="+", type=Path)
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.scenario not in LIVE_SCENARIOS:
        return args
    try:
        if args.scenario == "rollout":
            if args.first_command is not None:
                raise ValueError("rollout takes --family-command FAMILY=SHELL, not --first-command")
            prepare_rollout(args)
        elif hasattr(args, "images_file"):
            images = list(args.image)
            if args.images_file is not None:
                images.extend(read_images_file(args.images_file))
            if not images:
                parser.error("at least one --image or --images-file entry is required")
            args.images = images
        else:
            args.images = [args.image]
        args.profile = args.profile or ("linux_host" if args.scenario == "rollout" else "container")
        if args.scenario != "rollout":
            args.first_command_argv = parse_command(
                "true" if args.first_command is None else args.first_command,
                name="--first-command")
        if args.sandbox_command is None and args.scenario == "rollout":
            # SandboxSpec.benchmark, as the integration creates, passes none.
            args.sandbox_command_argv = []
        elif args.sandbox_command is None:
            args.sandbox_command_argv = ["sleep", str(args.ttl_seconds)]
        elif args.sandbox_command.strip():
            args.sandbox_command_argv = parse_command(args.sandbox_command,
                                                      name="--sandbox-command")
        else:
            args.sandbox_command_argv = []
        if args.scenario == "density":
            args.tool_command_argv = parse_command(args.tool_command, name="--tool-command")
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    if args.run_id is not None and not RUN_ID.fullmatch(args.run_id):
        parser.error("--run-id must match " + RUN_ID.pattern)
    labels = {}
    for item in args.label:
        key, separator, value = item.partition("=")
        if not separator or not key:
            parser.error(f"--label must be KEY=VALUE: {item!r}")
        labels[key] = value
    args.labels = labels
    if args.scenario == "cold" and duplicate_images(args.images):
        parser.error("cold images must be distinct (a repeated image is warm): "
                     + ", ".join(duplicate_images(args.images)))
    if args.scenario == "rate" and args.warmup_seconds >= args.window_seconds:
        parser.error("--warmup-seconds must be shorter than --window-seconds")
    if args.scenario == "density" and args.step > args.max_resident:
        parser.error("--step cannot exceed --max-resident")
    if args.scenario == "rollout" and \
            args.ramp_seconds + args.turns * args.think_seconds[1] >= args.ttl_seconds:
        parser.error("--ttl-seconds must exceed --ramp-seconds plus --turns times the "
                     "longest think time")
    return args


def prepare_rollout(args: argparse.Namespace) -> None:
    """Sample the tasks, resolve their images and build their commands."""
    entries, provenance = load_selection(args.selection)
    mix = [kind.strip() for kind in args.turn_mix.split(",") if kind.strip()]
    if not mix or any(kind not in TURN_KINDS for kind in mix):
        raise ValueError("--turn-mix must list kinds from " + ", ".join(TURN_KINDS))
    family_overrides = parse_assignments(args.family_command, name="--family-command")
    turn_overrides = parse_assignments(args.turn_command, name="--turn-command",
                                       keys=TURN_KINDS)
    chosen = sample_tasks(entries, count=args.tasks, seed=args.seed,
                          weighting=args.sampling, families=args.family)
    tasks = [{"index": index, "family": entry["family"], "image": entry["image"],
              "task_name": entry.get("task_name"), "cached_kind": entry.get("cached_kind"),
              "erofs_bytes": entry.get("erofs_bytes"), **resolve_image(entry, args.image_source)}
             for index, entry in enumerate(chosen)]
    families = sorted({task["family"] for task in tasks})
    commands = rollout_commands(families, family_overrides=family_overrides,
                                turn_overrides=turn_overrides,
                                timeout_seconds=args.exec_timeout_seconds)
    fallback = sorted({task["family"] for task in tasks
                       if task["resolution"] == "prepared_reference_fallback"})
    args.images = sorted({task["reference"] for task in tasks})
    # Underscored: kept out of the report's scenario_arguments copy.
    args._rollout = {"tasks": tasks, "commands": commands, "turn_mix": mix, "conditions": {
        "selection": provenance,
        "sampling": {"tasks": args.tasks, "seed": args.seed, "weighting": args.sampling,
                     "families": list(args.family),
                     "sampled_per_family": {family: sum(task["family"] == family
                                                        for task in tasks)
                                            for family in families}},
        "image_source": args.image_source,
        "resolutions": {resolution: sum(task["resolution"] == resolution for task in tasks)
                        for resolution in sorted({task["resolution"] for task in tasks})},
        "resolution_gap": {"families": fallback, "detail": RECIPE_GAP} if fallback else None,
        "family_profiles": {family: dict(zip(("kind", "dirs"), FAMILY_PROFILES.get(
            family, ("generic", "")))) for family in families},
        "overrides": {"family_command": sorted(family_overrides),
                      "turn_command": sorted(turn_overrides)},
        "commands": commands, "turn_mix": mix,
    }}


# ---------------------------------------------------------------------------
# Live run against a gateway. The SDK is imported only here.


class ScenarioStopped(RuntimeError):
    pass


def import_sdk() -> Any:
    sdk_src = REPO_ROOT / "ucloud-sandboxes-sdk" / "src"
    if sdk_src.is_dir() and str(sdk_src) not in sys.path:
        sys.path.insert(0, str(sdk_src))
    try:
        import ucloud_sandboxes_sdk
    except ImportError as exc:
        raise SystemExit(
            "ucloud_sandboxes_sdk is not importable; install the SDK wheel or check out "
            f"ucloud-sandboxes-sdk next to this repository ({exc})"
        ) from exc
    return ucloud_sandboxes_sdk


def sdk_identity(sdk: Any) -> dict[str, Any]:
    client_module = sys.modules.get(f"{getattr(sdk, '__name__', '')}.client")
    client_file = getattr(client_module, "__file__", None)
    digest = None
    if client_file and Path(client_file).is_file():
        digest = hashlib.sha256(Path(client_file).read_bytes()).hexdigest()
    return {"version": getattr(sdk, "__version__", None),
            "module": getattr(sdk, "__file__", None), "client_sha256": digest}


def resolve_credentials(args: argparse.Namespace) -> tuple[str, str | None]:
    url = (args.gateway_url or "").strip()
    if not url:
        raise SystemExit("set --gateway-url or UCLOUD_SANDBOX_URL")
    if args.api_token_file is not None:
        token = args.api_token_file.read_text(encoding="utf-8").strip()
        if not token:
            raise SystemExit(f"empty token file: {args.api_token_file}")
    else:
        token = (os.environ.get("UCLOUD_SANDBOX_API_TOKEN") or "").strip() or None
    if token:
        _SECRETS.add(token)
    return url, token


def sanitized_arguments(args: argparse.Namespace) -> dict[str, Any]:
    return {key: (str(value) if isinstance(value, Path) else value)
            for key, value in sorted(vars(args).items()) if not key.startswith("_")}


def run_parallel(items: Sequence[Any], operation: Callable[[Any], Any], *, workers: int,
                 stop: threading.Event) -> list[Any]:
    """Run in order-preserving parallel; an interrupt stops new submissions."""
    if not items:
        return []

    def guarded(item: Any) -> Any:
        if stop.is_set():
            raise ScenarioStopped("scenario stopped before this item started")
        return operation(item)

    executor = ThreadPoolExecutor(max_workers=max(1, min(workers, len(items))))
    try:
        futures = [executor.submit(guarded, item) for item in items]
        return [future.result() for future in futures]
    except BaseException:
        stop.set()
        raise
    finally:
        executor.shutdown(wait=True, cancel_futures=True)


class OperatorApi:
    """Read-only operator view: GET /v1/nodes with the gateway control token."""

    def __init__(self, url: str, token: str, timeout_seconds: float) -> None:
        self.url = url.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds

    def nodes(self) -> list[dict[str, Any]]:
        request = urllib.request.Request(self.url + "/v1/nodes", headers={
            "Authorization": f"Bearer {self.token}", "Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
        nodes = payload.get("nodes") if isinstance(payload, dict) else None
        if not isinstance(nodes, list):
            raise ValueError("GET /v1/nodes returned no nodes[] list")
        return nodes


class CommandFailed(RuntimeError):
    def __init__(self, message: str, error_code: str) -> None:
        super().__init__(message)
        self.error_code = error_code


class LiveRun:
    def __init__(self, args: argparse.Namespace, sdk: Any, client: Any,
                 report: dict[str, Any]) -> None:
        self.args = args
        self.sdk = sdk
        self.client = client
        self.report = report
        self.run_id = report["run_id"]
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.counter = itertools.count()
        # Every ID is recorded before its create request so a create that
        # times out client-side but succeeds server-side is still deleted.
        self.owned: dict[str, str] = {}
        self.inline_delete_errors: list[str] = []
        self.operator: OperatorApi | None = None

    # -- sandbox primitives -------------------------------------------------

    def collector(self, rows: list[Any], operation: Callable[[Any], Any]) -> Callable[[Any], Any]:
        """Wrap an operation so completed rows survive an interrupted batch."""
        def collect(item: Any) -> Any:
            row = operation(item)
            with self.lock:
                rows.append(row)
            return row
        return collect

    def new_id(self, scenario: str) -> str:
        with self.lock:
            index = next(self.counter)
        return f"{self.run_id}-{scenario}-{index:05d}"

    def image(self, reference: str) -> Any:
        if self.args.image_kind == "name":
            return self.sdk.Image.from_name(reference)
        return self.sdk.Image.from_registry(reference)

    def spec(self, sandbox_id: str, reference: str, *, parkable: bool = False) -> Any:
        # linux_host is what SandboxSpec.benchmark builds: no security or
        # filesystem override, so the gateway's host defaults apply.
        profile = ({} if self.args.profile == "container" else
                   {"profile": self.args.profile, "security": None, "filesystem": None})
        return self.sdk.SandboxSpec(
            **profile,
            id=sandbox_id,
            image=self.image(reference),
            command=tuple(self.args.sandbox_command_argv),
            cpus=self.args.cpus,
            memory_mb=self.args.memory_mb,
            disk_mb=self.args.disk_mb,
            ttl_seconds=self.args.ttl_seconds,
            network=self.args.network,
            labels={**self.args.labels, "benchmark": "rl-scale",
                    "benchmark.run_id": self.run_id},
            parkable=parkable,
        )

    def run_command(self, sandbox_id: str, command: Sequence[str], *,
                    markers: bool = False) -> dict[str, Any]:
        """One exec: start and wait timed separately. Exec is never replayed."""
        started = time.perf_counter()
        handle = self.client.start_exec(sandbox_id, list(command))
        dispatched = time.perf_counter()
        result = handle.wait(timeout_seconds=self.args.exec_timeout_seconds)
        finished = time.perf_counter()
        row = {"seconds": finished - started, "start_seconds": dispatched - started,
               "wait_seconds": finished - dispatched, "exit_code": result.exit_code,
               "status": result.status, "ok": bool(result.success)}
        if markers:
            row["markers"] = parse_markers(getattr(result, "stdout", "") or "")
        if not result.success:
            row["error"] = redact(f"command {list(command)!r} failed: status={result.status} "
                                  f"exit_code={result.exit_code} stderr={result.stderr[-400:]!r}")
        return row

    def create_ready(self, reference: str, scenario: str, *, ready: bool = True,
                     t0: float | None = None, delete_after: bool = False,
                     parkable: bool = False) -> dict[str, Any]:
        sandbox_id = self.new_id(scenario)
        with self.lock:
            self.owned[sandbox_id] = "requested"
        row: dict[str, Any] = {"sandbox_id": sandbox_id, "image": reference, "ok": False,
                               "started_unix": time.time()}
        started = time.perf_counter()
        if t0 is not None:
            row["queued_seconds"] = started - t0
        try:
            handle = self.client.create_sandbox(
                self.spec(sandbox_id, reference, parkable=parkable),
                request_timeout_seconds=self.args.create_timeout_seconds,
            )
            created = time.perf_counter()
            with self.lock:
                self.owned[sandbox_id] = "created"
            response = getattr(handle, "create_response", None) or {}
            row.update(create_seconds=created - started, node=node_identity(response),
                       node_timings=response.get("timings") if isinstance(response, dict) else None)
            done = created
            if ready:
                exec_row = self.run_command(sandbox_id, self.args.first_command_argv)
                done = time.perf_counter()
                row.update(first_exec_seconds=exec_row["seconds"],
                           first_exec_start_seconds=exec_row["start_seconds"],
                           time_to_first_command_seconds=done - started)
                if not exec_row["ok"]:
                    raise RuntimeError(exec_row["error"])
            row["ok"] = True
            if t0 is not None:
                row["completion_seconds"] = done - t0
        except Exception as exc:
            row["error"] = safe_error(exc)
            row["failed_after_seconds"] = time.perf_counter() - started
            if t0 is not None:
                row["completion_seconds"] = time.perf_counter() - t0
        finally:
            if delete_after:
                error = self.delete_one(sandbox_id)
                if error:
                    with self.lock:
                        self.inline_delete_errors.append(error)
        emit("sandbox_" + ("ready" if row["ok"] else "failed"), sandbox_id=sandbox_id,
             image=reference, seconds=row.get("time_to_first_command_seconds",
                                              row.get("create_seconds")),
             error=row.get("error"))
        return row

    def delete_one(self, sandbox_id: str) -> str | None:
        deadline = time.monotonic() + self.args.cleanup_timeout_seconds
        attempt = 0
        while True:
            try:
                self.client.delete_sandbox(sandbox_id)
                break
            except self.sdk.SandboxApiError as exc:
                status = getattr(exc, "status_code", None)
                if status == 404:
                    break
                # Transport failures surface as status None in the SDK.
                if (status is not None and status not in TRANSIENT_STATUSES) or \
                        time.monotonic() >= deadline:
                    return f"delete {sandbox_id}: {safe_error(exc)}"
            except Exception as exc:
                if time.monotonic() >= deadline:
                    return f"delete {sandbox_id}: {safe_error(exc)}"
            attempt += 1
            time.sleep(min(2.0, 0.1 * attempt, max(0.0, deadline - time.monotonic())))
        with self.lock:
            self.owned[sandbox_id] = "deleted"
        return None

    # -- run-level discipline -----------------------------------------------

    def require_idle_fleet(self) -> None:
        records = [record for record in self.client.list_sandboxes()
                   if record_state(record) != "deleted"]
        self.report["conditions"]["preexisting_sandboxes"] = len(records)
        if records and not self.args.allow_occupied_fleet:
            raise RuntimeError(
                f"deployment has {len(records)} visible sandbox(es); the benchmark requires "
                "an idle fleet (pass --allow-occupied-fleet to measure anyway)"
            )

    def cleanup(self) -> None:
        with self.lock:
            pending = [sid for sid, state in self.owned.items() if state != "deleted"]
        stop = threading.Event()
        errors = [error for error in run_parallel(pending, self.delete_one, workers=16, stop=stop)
                  if error]
        remaining: list[str] = []
        owned = set(self.owned)
        deadline = time.monotonic() + min(60.0, self.args.cleanup_timeout_seconds)
        while owned:
            try:
                listed = {record_id(record) for record in self.client.list_sandboxes()
                          if record_state(record) != "deleted"}
            except Exception as exc:
                errors.append(f"cleanup inventory: {safe_error(exc)}")
                break
            remaining = sorted(owned & listed)
            if not remaining or time.monotonic() >= deadline:
                break
            time.sleep(1.0)
        all_errors = [*self.inline_delete_errors, *errors]
        self.report["cleanup"] = {
            "attempted": len(self.owned), "deleted": sum(
                state == "deleted" for state in self.owned.values()),
            "failed": len(errors), "remaining_owned_ids": remaining, "errors": all_errors,
        }
        # Inline delete failures that the final sweep repaired stay visible in
        # cleanup.errors but no longer fail the run.
        self.report["cleanup_errors"] = [*errors] + (
            [f"owned sandboxes remain after cleanup: {', '.join(remaining)}"] if remaining else [])
        emit("cleanup_complete", attempted=len(self.owned), failed=len(errors),
             remaining=len(remaining))


def scenario_cold(run: LiveRun) -> None:
    rows: list[dict[str, Any]] = []
    try:
        run_parallel(run.args.images, run.collector(
            rows, lambda image: run.create_ready(image, "cold", delete_after=True)),
            workers=run.args.concurrency, stop=run.stop)
    finally:
        run.report["metrics"]["cold_time_to_first_command"] = first_command_section(
            rows, images=list(run.args.images), concurrency=run.args.concurrency,
            cold_assertion=("caller-asserted: each image was never pulled by any node; "
                            "the harness cannot verify cache state"))


def scenario_warm(run: LiveRun) -> None:
    image = run.args.images[0]
    priming = None
    rows: list[dict[str, Any]] = []
    try:
        if not run.args.no_prime:
            priming = run.create_ready(image, "warm-prime", delete_after=True)
        for _ in range(run.args.repeats):
            if run.stop.is_set():
                break
            rows.append(run.create_ready(image, "warm", delete_after=True))
    finally:
        run.report["metrics"]["warm_time_to_first_command"] = first_command_section(
            rows, image=image, repeats=run.args.repeats, priming=priming,
            warm_definition="the same image repeated sequentially after an unmeasured "
                            "priming create (unless --no-prime); each sandbox is deleted "
                            "before the next create")


def scenario_burst(run: LiveRun) -> None:
    assignment = assign_images(run.args.sandboxes, run.args.images)
    rows: list[dict[str, Any]] = []
    t0 = time.perf_counter()
    emit("burst_started", sandboxes=len(assignment), images=len(set(assignment)))
    try:
        run_parallel(assignment, run.collector(
            rows, lambda image: run.create_ready(image, "burst", t0=t0)),
            workers=run.args.concurrency, stop=run.stop)
    finally:
        run.report["metrics"]["burst_completion"] = burst_section(
            rows, images=run.args.images, concurrency=run.args.concurrency)


def scenario_rate(run: LiveRun) -> None:
    args = run.args
    images = itertools.cycle(args.images)
    rows: list[dict[str, Any]] = []
    launched = 0
    cap_reached: list[float] = []
    t0 = time.perf_counter()
    deadline = t0 + args.window_seconds

    def worker() -> None:
        nonlocal launched
        while not run.stop.is_set() and time.perf_counter() < deadline:
            with run.lock:
                if launched >= args.max_sandboxes:
                    if not cap_reached:
                        cap_reached.append(time.perf_counter() - t0)
                    return
                launched += 1
                image = next(images)
            row = run.create_ready(image, "rate", ready=args.ready, t0=t0)
            with run.lock:
                rows.append(row)

    threads = [threading.Thread(target=worker, name=f"rate-{index}", daemon=True)
               for index in range(args.concurrency)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            while thread.is_alive():
                thread.join(0.5)
    except BaseException:
        run.stop.set()
        for thread in threads:
            thread.join()
        raise
    finally:
        with run.lock:
            snapshot = sorted(rows, key=lambda row: row.get("completion_seconds", 0.0))
        run.report["metrics"]["creation_rate"] = rate_section(
            snapshot, window_seconds=args.window_seconds, warmup_seconds=args.warmup_seconds,
            bin_seconds=args.bin_seconds,
            counted_event="first-command readiness" if args.ready else "create",
            cap_reached_seconds=cap_reached[0] if cap_reached else None,
            max_sandboxes=args.max_sandboxes, concurrency=args.concurrency)


def scenario_density(run: LiveRun, *, persist: Callable[[], None]) -> None:
    args = run.args
    image = args.images[0]
    residents: list[str] = []
    samples: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []
    stop_reason = None

    def tool_call(item: tuple[int, str]) -> dict[str, Any]:
        resident, sandbox_id = item
        sample: dict[str, Any] = {"resident": resident, "sandbox_id": sandbox_id, "ok": False}
        try:
            sample.update(run.run_command(sandbox_id, args.tool_command_argv))
        except Exception as exc:
            sample["error"] = safe_error(exc)
        return sample

    def record() -> None:
        run.report["metrics"]["density_at_latency"] = density_section(
            samples, bin_width=args.step, limit_seconds=args.p99_limit_seconds,
            tool_command=args.tool_command_argv, steps=steps, stop_reason=stop_reason)

    try:
        target = args.step
        while target <= args.max_resident and not run.stop.is_set():
            created: list[dict[str, Any]] = []
            run_parallel(list(range(target - len(residents))), run.collector(
                created, lambda _: run.create_ready(image, "density")),
                workers=args.create_concurrency, stop=run.stop)
            residents.extend(row["sandbox_id"] for row in created if row["ok"])
            failures = [row for row in created if not row["ok"]]
            step = {"target": target, "resident": len(residents), "created": len(created),
                    "create_failures": len(failures),
                    "create": latency_summary(row["time_to_first_command_seconds"]
                                              for row in created if row["ok"])}
            steps.append(step)
            if failures:
                stop_reason = f"create failures at target {target}: {failures[0].get('error')}"
                break
            plan = [(len(residents), residents[index % len(residents)])
                    for index in range(args.probes_per_bin)]
            batch: list[dict[str, Any]] = []
            try:
                run_parallel(plan, run.collector(batch, tool_call),
                             workers=args.probe_concurrency, stop=run.stop)
            finally:
                samples.extend(batch)
            ok = [row["seconds"] for row in batch if row.get("ok")]
            step["tool"] = latency_summary(ok)
            step["tool_failures"] = len(batch) - len(ok)
            record()
            persist()
            emit("density_bin", resident=len(residents), p99=step["tool"]["p99"],
                 failures=step["tool_failures"])
            violated = step["tool_failures"] or step["tool"]["p99"] is None or \
                step["tool"]["p99"] > args.p99_limit_seconds
            if violated and not args.continue_past_limit:
                stop_reason = f"p99 limit exceeded at {len(residents)} residents"
                break
            target += args.step
        else:
            stop_reason = stop_reason or ("interrupted" if run.stop.is_set()
                                          else "max resident reached")
    finally:
        record()


def scenario_park(run: LiveRun) -> None:
    methods = sdk_park_wake_methods(run.client)
    if methods is None:
        run.report["metrics"]["pause_resume"] = unsupported_section(
            f"ucloud_sandboxes_sdk {getattr(run.sdk, '__version__', '?')} exposes no "
            "park/wake (pause/resume) method; the gateway park/wake routes require the "
            "operator control token (docs/api-reference.md) and are exercised by "
            "scripts/live_park_resume_benchmark.py",
            sdk_methods_checked=[list(pair) for pair in PARK_WAKE_METHOD_PAIRS],
            bytes_written=None, bytes_written_source=PAUSE_BYTES_SOURCE)
        return
    park_name, wake_name = methods
    image = run.args.images[0]
    cycles: list[dict[str, Any]] = []
    created = None
    try:
        created = run.create_ready(image, "park", parkable=True)
        if not created["ok"]:
            raise RuntimeError(created.get("error") or "parkable sandbox create failed")
        sandbox_id = created["sandbox_id"]
        for cycle in range(run.args.cycles):
            if run.stop.is_set():
                break
            started = time.perf_counter()
            getattr(run.client, park_name)(sandbox_id)
            parked = time.perf_counter()
            getattr(run.client, wake_name)(sandbox_id)
            woken = time.perf_counter()
            tool = run.run_command(sandbox_id, run.args.first_command_argv)
            if not tool["ok"]:
                raise RuntimeError(tool["error"])
            cycles.append({"cycle": cycle, "pause_seconds": parked - started,
                           "resume_seconds": woken - parked,
                           "resume_to_first_command_seconds": woken - parked + tool["seconds"]})
    finally:
        ok = bool(cycles)
        run.report["metrics"]["pause_resume"] = {
            "status": "measured" if ok else "failed",
            "unit": "seconds",
            "sdk_methods": [park_name, wake_name],
            "pause": latency_summary(row["pause_seconds"] for row in cycles),
            "resume": latency_summary(row["resume_seconds"] for row in cycles),
            "resume_to_first_command": latency_summary(
                row["resume_to_first_command_seconds"] for row in cycles),
            "bytes_written": None, "bytes_written_source": PAUSE_BYTES_SOURCE,
            "sandbox": created, "samples": cycles,
        }


def rollout_sandbox(run: LiveRun, task: dict[str, Any], *, t0: float, offset: float,
                    inflight: dict[str, int]) -> dict[str, Any]:
    """One rollout: create, the family's first command, M turns, delete."""
    args, plan = run.args, run.args._rollout
    sandbox_id = run.new_id("rollout")
    with run.lock:
        run.owned[sandbox_id] = "requested"
    row: dict[str, Any] = {
        "sandbox_id": sandbox_id, "ok": False, "phase": "create",
        **{key: task.get(key) for key in ("family", "image", "task_name", "reference",
                                           "cached_kind", "erofs_bytes")},
        "image_resolution": task["resolution"], "arrival_offset_seconds": round(offset, 6),
        "turns": []}
    started = time.perf_counter()
    row["start_offset_seconds"] = started - t0

    def check(result: dict[str, Any], phase: str) -> None:
        if not result["ok"]:
            code = result["exit_code"] if result["exit_code"] is not None else result["status"]
            raise CommandFailed(result["error"], f"{phase}_exit_{code}")

    try:
        with run.lock:
            inflight["now"] += 1
            inflight["max"] = max(inflight["max"], inflight["now"])
        try:
            handle = run.client.create_sandbox(
                run.spec(sandbox_id, task["reference"], parkable=args.parkable),
                request_timeout_seconds=args.create_timeout_seconds)
        finally:
            with run.lock:
                inflight["now"] -= 1
        with run.lock:
            run.owned[sandbox_id] = "created"
        response = getattr(handle, "create_response", None) or {}
        row.update(time_to_ready_seconds=time.perf_counter() - started,
                   node=node_identity(response), create_phases_ms=create_phases(
                       response.get("timings") if isinstance(response, dict) else None),
                   phase="first_command")
        first = run.run_command(sandbox_id, plan["commands"]["first"][task["family"]],
                                markers=True)
        row.update(first_command_seconds=first["seconds"],
                   first_command_markers=first.get("markers"))
        check(first, "first_command")
        row.update(first_command_ok=True, phase="turn",
                   time_to_first_command_seconds=time.perf_counter() - started)
        rng = random.Random(f"{args.seed}:{task['index']}")
        for turn in range(args.turns):
            if run.stop.wait(rng.uniform(*args.think_seconds)):
                raise ScenarioStopped("interrupted during think time")
            kind = plan["turn_mix"][(task["index"] + turn) % len(plan["turn_mix"])]
            result = run.run_command(sandbox_id, plan["commands"]["turns"][kind], markers=True)
            row["turns"].append({"turn": turn, "kind": kind, "ok": result["ok"],
                                 "seconds": result["seconds"], "markers": result.get("markers"),
                                 "offset_seconds": time.perf_counter() - t0})
            check(result, f"turn_{kind}")
        row.update(ok=True, phase="done")
    except Exception as exc:
        row.update(error=safe_error(exc), error_code=error_code_of(exc),
                   failed_after_seconds=time.perf_counter() - started)
    finally:
        # The episode ends with its sandbox, freeing capacity for later arrivals.
        error = run.delete_one(sandbox_id)
        if error:
            with run.lock:
                run.inline_delete_errors.append(error)
        row["finished_offset_seconds"] = time.perf_counter() - t0
    emit("rollout_" + ("done" if row["ok"] else "failed"), sandbox_id=sandbox_id,
         family=row["family"], ready=row.get("time_to_ready_seconds"),
         first_command=row.get("time_to_first_command_seconds"), error=row.get("error"))
    return row


def observe_fleet(run: LiveRun) -> dict[str, Any] | None:
    if run.operator is None:
        return None
    return fleet_snapshot(run.operator.nodes(), now=time.time(),
                          fresh_seconds=run.args.node_fresh_seconds)


def scenario_rollout(run: LiveRun) -> None:
    args, plan = run.args, run.args._rollout
    tasks = plan["tasks"]
    conditions = run.report["conditions"]["rollout"] = dict(plan["conditions"])
    before = observe_fleet(run)
    fleet: dict[str, Any] = {"declared": args.fleet_state, "verified": None, "before": before}
    conditions["fleet_state"] = fleet
    if before is None:
        fleet["note"] = "declared only: no --operator-token-file to read node heartbeats"
    elif args.fleet_state == "zero":
        fleet["verified"] = before["sandbox_nodes_fresh"] == 0
        if not fleet["verified"]:
            raise RuntimeError(
                f"--fleet-state zero, but {before['sandbox_nodes_fresh']} sandbox node(s) "
                f"heartbeat within {args.node_fresh_seconds:g} s; scale the fleet to zero "
                "first (this harness never changes it)")
    else:
        # Cache state is declared; an empty cache shows as zero cached bytes.
        fleet["consistent"] = before["sandbox_nodes_fresh"] > 0 and (
            args.fleet_state == "warm-seeded" or before["cached_bytes_fresh"] == 0)
    count = len(tasks)
    offsets = [args.ramp_seconds * index / count for index in range(count)]
    rows: list[dict[str, Any]] = []
    timeline: list[dict[str, Any]] = []
    go, poll_stop = threading.Event(), threading.Event()
    start = [0.0]
    inflight = {"now": 0, "max": 0}

    def worker(index: int) -> None:
        go.wait()
        if run.stop.wait(max(0.0, start[0] + offsets[index] - time.perf_counter())):
            return
        row = rollout_sandbox(run, tasks[index], t0=start[0], offset=offsets[index],
                              inflight=inflight)
        with run.lock:
            rows.append(row)

    def poll() -> None:
        while not poll_stop.wait(args.node_poll_seconds):
            entry: dict[str, Any] = {"offset_seconds": round(time.perf_counter() - start[0], 3)}
            try:
                entry["sandbox_nodes_fresh"] = observe_fleet(run)["sandbox_nodes_fresh"]
            except Exception as exc:
                entry["error"] = safe_error(exc)
            timeline.append(entry)

    # One thread per sandbox: the client never caps concurrency below N.
    threads = [threading.Thread(target=worker, args=(index,), name=f"rollout-{index}",
                                daemon=True) for index in range(count)]
    poller = (threading.Thread(target=poll, name="rollout-nodes", daemon=True)
              if run.operator is not None and args.node_poll_seconds > 0 else None)
    try:
        for thread in threads:
            thread.start()
        start[0] = time.perf_counter()
        go.set()
        emit("rollout_started", tasks=count, references=len(args.images),
             fleet_state=args.fleet_state)
        if poller is not None:
            poller.start()
        for thread in threads:
            while thread.is_alive():
                thread.join(0.5)
    except BaseException:
        run.stop.set()
        go.set()
        for thread in threads:
            if thread.ident is not None:
                thread.join()
        raise
    finally:
        poll_stop.set()
        wall = time.perf_counter() - start[0] if start[0] else 0.0
        if poller is not None and poller.ident is not None:
            poller.join(args.request_timeout_seconds)
        if run.operator is None:
            node_io: dict[str, Any] = {"status": "not_measured", "reason": (
                "no --operator-token-file: node heartbeats need the gateway control token")}
        else:
            run.stop.wait(args.heartbeat_settle_seconds)
            try:
                after = observe_fleet(run)
                node_io = {
                    "status": "measured", "after": after,
                    "sandbox_nodes_fresh": {"before": before["sandbox_nodes_fresh"],
                                            "after": after["sandbox_nodes_fresh"]},
                    "node_count_timeline": timeline,
                    "first_fresh_node_offset_seconds": next(
                        (entry["offset_seconds"] for entry in timeline
                         if entry.get("sandbox_nodes_fresh")), None),
                    "environment_io": environment_io_deltas(before, after)}
            except Exception as exc:
                node_io = {"status": "failed", "error": safe_error(exc),
                           "node_count_timeline": timeline}
        with run.lock:
            snapshot = sorted(rows, key=lambda row: row["sandbox_id"])
        run.report["metrics"]["rollout"] = rollout_section(
            snapshot, tasks=count, fleet_state=args.fleet_state, node_io=node_io,
            arrival={"ramp_seconds": args.ramp_seconds, "client_concurrency": count,
                     "max_concurrent_creates": inflight["max"], "wall_seconds": round(wall, 6),
                     "turns": args.turns, "think_seconds": list(args.think_seconds),
                     "turn_mix": plan["turn_mix"], "parkable": args.parkable,
                     "profile": args.profile})


def build_conditions(args: argparse.Namespace, sdk: Any, url: str,
                     health: object) -> dict[str, Any]:
    return {
        "gateway": gateway_origin(url),
        "gateway_health": health,
        "gateway_version": health.get("version") if isinstance(health, dict) else None,
        "sdk": sdk_identity(sdk),
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "driver": {"python": sys.version.split()[0], "platform": platform.platform(),
                   "host": platform.node()},
        "images": list(args.images),
        "image_kind": args.image_kind,
        "shape": {"cpus": args.cpus, "memory_mb": args.memory_mb, "disk_mb": args.disk_mb,
                  "ttl_seconds": args.ttl_seconds, "network": args.network,
                  "sandbox_command": list(args.sandbox_command_argv)},
        "first_command": (list(args.first_command_argv) if hasattr(args, "first_command_argv")
                          else "per family: conditions.rollout.commands.first"),
        "profile": args.profile,
        "scenario_arguments": sanitized_arguments(args),
    }


def _raise_interrupt(signum: int, _frame: object) -> None:
    raise KeyboardInterrupt(f"signal {signum}")


def run_live(args: argparse.Namespace, *, sdk: Any = None, client: Any = None,
             operator: OperatorApi | None = None) -> dict[str, Any]:
    sdk = sdk if sdk is not None else import_sdk()
    url, token = resolve_credentials(args)
    if client is None:
        client = sdk.SandboxClient(url, api_token=token,
                                   timeout_seconds=args.request_timeout_seconds)
    if operator is None and getattr(args, "operator_token_file", None) is not None:
        operator_token = args.operator_token_file.read_text(encoding="utf-8").strip()
        if not operator_token:
            raise SystemExit(f"empty token file: {args.operator_token_file}")
        _SECRETS.add(operator_token)
        operator = OperatorApi(url, operator_token, args.request_timeout_seconds)
    # Reserve only after configuration is known to be usable.
    reserve_output(args.output, overwrite=args.overwrite)
    run_id = args.run_id or "rlbench-" + uuid4().hex[:12]
    report = new_report(run_id, args.scenario, {"scenario_arguments": sanitized_arguments(args)})
    run = LiveRun(args, sdk, client, report)
    run.operator = operator
    interrupted = False

    def persist() -> None:
        write_report(args.output, report)

    previous = None
    if threading.current_thread() is threading.main_thread():
        previous = signal.signal(signal.SIGTERM, _raise_interrupt)
    emit("benchmark_started", run_id=run_id, scenario=args.scenario)
    try:
        health = client.health()
        report["conditions"] = build_conditions(args, sdk, url, health)
        persist()
        if not (args.scenario == "park" and sdk_park_wake_methods(client) is None):
            run.require_idle_fleet()
        runner = {"cold": scenario_cold, "warm": scenario_warm, "burst": scenario_burst,
                  "rate": scenario_rate, "park": scenario_park,
                  "rollout": scenario_rollout}.get(args.scenario)
        if args.scenario == "density":
            scenario_density(run, persist=persist)
        else:
            runner(run)
    except KeyboardInterrupt:
        interrupted = True
        run.stop.set()
        report["errors"].append("interrupted")
    except Exception as exc:
        run.stop.set()
        report["errors"].append(safe_error(exc))
    finally:
        try:
            run.cleanup()
        except BaseException as exc:
            interrupted = interrupted or isinstance(exc, KeyboardInterrupt)
            report["cleanup_errors"].append("cleanup aborted: " + safe_error(exc))
        if previous is not None:
            signal.signal(signal.SIGTERM, previous)
        finalize_report(report, interrupted=interrupted)
        problems = validate_report(report)
        if problems:
            report["errors"].append("report validation: " + "; ".join(problems))
            finalize_report(report, interrupted=interrupted)
        persist()
        emit("benchmark_finished", run_id=run_id, status=report["status"],
             errors=len(report["errors"]), cleanup_errors=len(report["cleanup_errors"]))
    return report


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.scenario == "validate":
        failed = False
        for path in args.reports:
            try:
                problems = validate_report(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, ValueError) as exc:
                problems = [str(exc)]
            print(json.dumps({"report": str(path), "valid": not problems, "problems": problems}))
            failed |= bool(problems)
        return 1 if failed else 0
    if args.scenario == "merge":
        try:
            inputs = [(str(path), json.loads(path.read_text(encoding="utf-8")))
                      for path in args.reports]
            merged = merge_reports(inputs)
        except (OSError, ValueError) as exc:
            print(json.dumps({"merged": False, "error": str(exc)}))
            return 1
        reserve_output(args.output, overwrite=args.overwrite)
        write_report(args.output, merged)
        print(json.dumps({"merged": True, "output": str(args.output), "ok": merged["ok"]}))
        return 0 if merged["ok"] else 1
    report = run_live(args)
    if report["status"] == "interrupted":
        return 130
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
