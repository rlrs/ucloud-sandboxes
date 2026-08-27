#!/usr/bin/env python3
"""Emit one agent-readable UCloud production diagnostics snapshot."""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


SCHEMA_VERSION = 1
VICTORIA_URL = "http://127.0.0.1:8428"
TEMPO_URL = "http://127.0.0.1:3200"
GRAFANA_URL = "http://127.0.0.1:3000"
COLLECTOR_URL = "http://127.0.0.1:13133"
DATA_ROOT = Path("/var/lib/ucloud-sandboxes/telemetry")
DEPLOYMENT_CONFIG = Path("/etc/ucloud-sandboxes/deployment.json")
SERVICES = (
    "ucloud-telemetry-tempo",
    "ucloud-telemetry-victoria",
    "ucloud-telemetry-collector",
    "ucloud-telemetry-grafana",
    "ucloud-sandbox-gateway",
    "ucloud-sandbox-autoscaler",
    "ucloud-sandbox-relay",
)
CONTAINERS = (
    "ucloud-telemetry-tempo",
    "ucloud-telemetry-victoria",
    "ucloud-telemetry-collector",
    "ucloud-telemetry-grafana",
)
WINDOW_RE = re.compile(r"^[1-9][0-9]*[smhd]$")


def _json_request(url: str, *, timeout: float = 10.0) -> Any:
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def _safe_json_request(url: str, *, timeout: float = 10.0) -> dict[str, Any]:
    started = time.monotonic()
    try:
        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=timeout) as response:
            raw = response.read()
        try:
            body: Any = json.loads(raw)
        except json.JSONDecodeError:
            body = raw.decode("utf-8", errors="replace").strip()
        return {
            "ok": True,
            "latency_ms": round((time.monotonic() - started) * 1_000, 3),
            "response": body,
        }
    except (HTTPError, URLError, TimeoutError) as exc:
        return {
            "ok": False,
            "latency_ms": round((time.monotonic() - started) * 1_000, 3),
            "error": f"{type(exc).__name__}: {exc}",
        }


def _prom_query(expression: str) -> list[dict[str, Any]]:
    url = f"{VICTORIA_URL}/api/v1/query?{urlencode({'query': expression})}"
    payload = _json_request(url)
    if payload.get("status") != "success":
        raise RuntimeError(f"VictoriaMetrics query failed: {payload}")
    data = payload.get("data", {})
    if not isinstance(data, dict) or not isinstance(data.get("result"), list):
        raise RuntimeError(f"VictoriaMetrics returned an invalid result: {payload}")
    return data["result"]


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return round(number, 9) if math.isfinite(number) else None


def _instant_series(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        metric = row.get("metric", {})
        value = row.get("value", [None, None])
        if not isinstance(metric, dict) or not isinstance(value, list) or len(value) < 2:
            continue
        result.append(
            {
                "labels": {str(key): str(item) for key, item in metric.items()},
                "value": _number(value[1]),
            }
        )
    return result


def _metrics_snapshot(window: str, rate_window: str) -> dict[str, Any]:
    counts = _instant_series(
        _prom_query(
            "sum by (service_name, operation, status) "
            f"(increase(ucloud_platform_operation_count_total[{window}]))"
        )
    )
    rates = _instant_series(
        _prom_query(
            "sum by (service_name, operation, status) "
            f"(rate(ucloud_platform_operation_count_total[{rate_window}]))"
        )
    )
    cumulative_counts = _instant_series(
        _prom_query(
            "sum by (service_name, operation, status) "
            "(ucloud_platform_operation_count_total)"
        )
    )
    latency: dict[str, list[dict[str, Any]]] = {}
    cumulative_latency: dict[str, list[dict[str, Any]]] = {}
    for quantile in ("0.50", "0.95", "0.99"):
        rows = _instant_series(
            _prom_query(
                f"histogram_quantile({quantile}, "
                "sum by (le, service_name, operation) "
                f"(rate(ucloud_platform_operation_duration_seconds_bucket[{rate_window}])))"
            )
        )
        latency[f"p{int(float(quantile) * 100)}_seconds"] = sorted(
            (row for row in rows if row["value"] is not None),
            key=lambda row: row["value"],
            reverse=True,
        )
        cumulative_rows = _instant_series(
            _prom_query(
                f"histogram_quantile({quantile}, "
                "sum by (le, service_name, operation) "
                "(ucloud_platform_operation_duration_seconds_bucket))"
            )
        )
        cumulative_latency[f"p{int(float(quantile) * 100)}_seconds"] = sorted(
            (row for row in cumulative_rows if row["value"] is not None),
            key=lambda row: row["value"],
            reverse=True,
        )
    errors = [
        row
        for row in counts
        if row["labels"].get("status") == "error" and (row["value"] or 0) > 0
    ]
    services = sorted(
        {
            row["labels"]["service_name"]
            for row in counts
            if row["labels"].get("service_name")
        }
    )
    return {
        "window": window,
        "rate_window": rate_window,
        "reporting_services": services,
        "operation_counts": sorted(
            counts,
            key=lambda row: row["value"] if row["value"] is not None else -1,
            reverse=True,
        ),
        "cumulative_operation_counts": sorted(
            cumulative_counts,
            key=lambda row: row["value"] if row["value"] is not None else -1,
            reverse=True,
        ),
        "operation_rates_per_second": sorted(
            rates,
            key=lambda row: row["value"] if row["value"] is not None else -1,
            reverse=True,
        ),
        "operation_errors": sorted(
            errors,
            key=lambda row: row["value"] if row["value"] is not None else -1,
            reverse=True,
        ),
        "latency": latency,
        "cumulative_latency_since_process_start": cumulative_latency,
    }


def _attribute_value(raw: Any) -> Any:
    if not isinstance(raw, dict):
        return None
    for key in (
        "stringValue",
        "boolValue",
        "intValue",
        "doubleValue",
        "bytesValue",
    ):
        if key in raw:
            return raw[key]
    if "arrayValue" in raw:
        values = raw["arrayValue"].get("values", [])
        return [_attribute_value(item) for item in values]
    return None


def _attributes(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, list):
        return {}
    result: dict[str, Any] = {}
    for item in raw:
        if isinstance(item, dict) and isinstance(item.get("key"), str):
            result[item["key"]] = _attribute_value(item.get("value"))
    return result


def _span_id(raw: Any) -> str:
    if not isinstance(raw, str) or not raw:
        return ""
    try:
        return base64.b64decode(raw).hex()
    except (ValueError, TypeError):
        return raw


def _trace_detail(trace_id: str) -> dict[str, Any]:
    payload = _json_request(f"{TEMPO_URL}/api/traces/{trace_id}")
    spans: list[dict[str, Any]] = []
    services: set[str] = set()
    for batch in payload.get("batches", []):
        resource = _attributes(batch.get("resource", {}).get("attributes", []))
        service = str(resource.get("service.name", "unknown"))
        services.add(service)
        for scope in batch.get("scopeSpans", []):
            for span in scope.get("spans", []):
                start_ns = int(span.get("startTimeUnixNano", 0))
                end_ns = int(span.get("endTimeUnixNano", start_ns))
                duration_ms = max(0.0, (end_ns - start_ns) / 1_000_000)
                attributes = _attributes(span.get("attributes", []))
                thread_cpu_seconds = _number(
                    attributes.get("ucloud.span.thread_cpu.duration")
                )
                thread_cpu_ms = (
                    round(thread_cpu_seconds * 1_000, 6)
                    if thread_cpu_seconds is not None
                    else None
                )
                status = span.get("status", {})
                status_code = (
                    status.get("code", "STATUS_CODE_UNSET")
                    if isinstance(status, dict)
                    else "STATUS_CODE_UNSET"
                )
                spans.append(
                    {
                        "service": service,
                        "name": span.get("name", ""),
                        "span_id": _span_id(span.get("spanId")),
                        "parent_span_id": _span_id(span.get("parentSpanId")),
                        "duration_ms": round(duration_ms, 6),
                        "thread_cpu_ms": thread_cpu_ms,
                        "cpu_wall_ratio": (
                            round(thread_cpu_ms / duration_ms, 6)
                            if thread_cpu_ms is not None and duration_ms > 0
                            else None
                        ),
                        "status": status_code,
                        "attributes": attributes,
                    }
                )
    errors = [
        span
        for span in spans
        if span["status"] == "STATUS_CODE_ERROR"
        or "error.type" in span["attributes"]
    ]
    return {
        "trace_id": trace_id,
        "services": sorted(services),
        "span_count": len(spans),
        "error_spans": errors,
        "longest_spans": sorted(
            spans, key=lambda span: span["duration_ms"], reverse=True
        )[:20],
    }


def _trace_search(query: str, *, start: int, end: int) -> list[dict[str, Any]]:
    parameters = {
        "q": query,
        "start": str(start),
        "end": str(end),
        "limit": "100",
    }
    payload = _json_request(f"{TEMPO_URL}/api/search?{urlencode(parameters)}")
    return sorted(
        (item for item in payload.get("traces", []) if isinstance(item, dict)),
        key=lambda item: int(item.get("durationMs", 0)),
        reverse=True,
    )


def _traces_snapshot(window_seconds: int, trace_limit: int) -> dict[str, Any]:
    now = int(time.time())
    start = now - window_seconds
    slow_query = "{ duration > 10ms }"
    error_query = "{ status = error }"
    summaries = _trace_search(slow_query, start=start, end=now)
    error_summaries = _trace_search(error_query, start=start, end=now)
    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    for summary in (*error_summaries, *summaries):
        trace_id = summary.get("traceID")
        if not isinstance(trace_id, str) or not trace_id or trace_id in selected_ids:
            continue
        selected.append(summary)
        selected_ids.add(trace_id)
        if len(selected) >= trace_limit:
            break
    details: list[dict[str, Any]] = []
    detail_errors: list[dict[str, str]] = []
    for summary in selected:
        trace_id = summary.get("traceID")
        if not isinstance(trace_id, str) or not trace_id:
            continue
        try:
            details.append(_trace_detail(trace_id))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            detail_errors.append(
                {"trace_id": trace_id, "error": f"{type(exc).__name__}: {exc}"}
            )
    return {
        "slow_query": slow_query,
        "error_query": error_query,
        "searched_trace_count": len(summaries),
        "error_trace_count": len(error_summaries),
        "error_traces": error_summaries[:20],
        "slowest_traces": summaries[:20],
        "detailed_traces": details,
        "detail_errors": detail_errors,
    }


def _meminfo() -> dict[str, int | float | None]:
    values: dict[str, int] = {}
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        key, raw = line.split(":", 1)
        match = re.search(r"[0-9]+", raw)
        if match:
            values[key] = int(match.group()) * 1024
    total = values.get("MemTotal", 0)
    available = values.get("MemAvailable", 0)
    return {
        "total_bytes": total,
        "available_bytes": available,
        "available_ratio": round(available / total, 6) if total else None,
        "swap_total_bytes": values.get("SwapTotal", 0),
        "swap_free_bytes": values.get("SwapFree", 0),
    }


def _disk() -> dict[str, int | float]:
    stats = os.statvfs(DATA_ROOT)
    total = stats.f_blocks * stats.f_frsize
    available = stats.f_bavail * stats.f_frsize
    return {
        "path": str(DATA_ROOT),
        "total_bytes": total,
        "available_bytes": available,
        "available_ratio": round(available / total, 6) if total else 0.0,
    }


def _systemd_services() -> dict[str, str]:
    result: dict[str, str] = {}
    for service in SERVICES:
        completed = subprocess.run(
            ["systemctl", "is-active", service],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        result[service] = completed.stdout.strip() or "unknown"
    return result


def _container_stats() -> list[dict[str, Any]]:
    completed = subprocess.run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{json .}}",
            *CONTAINERS,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    if completed.returncode:
        return [{"error": completed.stderr.strip() or "docker stats failed"}]
    result: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        result.append(
            {
                "name": item.get("Name"),
                "cpu_percent": _number(str(item.get("CPUPerc", "")).rstrip("%")),
                "memory_usage": item.get("MemUsage"),
                "memory_percent": _number(
                    str(item.get("MemPerc", "")).rstrip("%")
                ),
                "pids": int(item["PIDs"]) if str(item.get("PIDs", "")).isdigit() else None,
            }
        )
    return result


def _config_summary() -> dict[str, Any]:
    if not DEPLOYMENT_CONFIG.is_file():
        return {"error": f"{DEPLOYMENT_CONFIG} is absent"}
    payload = json.loads(DEPLOYMENT_CONFIG.read_text(encoding="utf-8"))
    telemetry = payload.get("telemetry", {})
    return {
        "deployment_id": payload.get("deployment_id"),
        "provider": payload.get("provider", {}).get("kind"),
        "telemetry": telemetry,
    }


def _findings(report: dict[str, Any]) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for service, state in report["host"]["systemd_services"].items():
        if state != "active":
            findings.append(
                {
                    "severity": "critical",
                    "kind": "service_unhealthy",
                    "message": f"{service} is {state}",
                }
            )
    for name, health in report["backends"].items():
        if not health.get("ok"):
            findings.append(
                {
                    "severity": "critical",
                    "kind": "backend_unhealthy",
                    "message": f"{name} health check failed: {health.get('error')}",
                }
            )
    available_ratio = report["host"]["memory"].get("available_ratio")
    if available_ratio is not None and available_ratio < 0.15:
        findings.append(
            {
                "severity": "warning",
                "kind": "memory_pressure",
                "message": f"Only {available_ratio:.1%} of gateway memory is available",
            }
        )
    disk = report["host"]["telemetry_disk"]
    if disk["available_bytes"] < 25 * 1024**3:
        findings.append(
            {
                "severity": "warning",
                "kind": "telemetry_disk_pressure",
                "message": "Telemetry disk has less than 25 GiB available",
            }
        )
    operation_counts = report.get("metrics", {}).get("operation_counts", [])
    successful_counts = {
        (
            row["labels"].get("service_name"),
            row["labels"].get("operation"),
        ): row["value"]
        for row in operation_counts
        if row["labels"].get("status") == "ok" and row["value"] is not None
    }
    for row in report.get("metrics", {}).get("operation_errors", []):
        labels = row["labels"]
        key = (labels.get("service_name"), labels.get("operation"))
        successful = successful_counts.get(key, 0) or 0
        if successful > 0:
            kind = "operation_errors_with_success"
            suffix = (
                f" and {successful} successful completions; this indicates "
                "retry/recovery activity rather than a continuously failing operation"
            )
        else:
            kind = "operation_errors"
            suffix = " with no successful completion in the analysis window"
        findings.append(
            {
                "severity": "warning",
                "kind": kind,
                "message": (
                    f"{labels.get('service_name', 'unknown')}:"
                    f"{labels.get('operation', 'unknown')} recorded {row['value']} errors"
                    f"{suffix}"
                ),
            }
        )
    if not report.get("metrics", {}).get("reporting_services"):
        findings.append(
            {
                "severity": "warning",
                "kind": "telemetry_absent",
                "message": "No platform metric series were present in the requested window",
            }
        )
    if not findings:
        findings.append(
            {
                "severity": "info",
                "kind": "no_immediate_fault",
                "message": "No service, backend, resource-pressure, or operation-error fault was detected",
            }
        )
    return findings


def create_report(
    window: str, *, rate_window: str = "5m", trace_limit: int
) -> dict[str, Any]:
    if not WINDOW_RE.fullmatch(window):
        raise ValueError("window must be a positive Prometheus duration such as 15m or 1h")
    if not WINDOW_RE.fullmatch(rate_window):
        raise ValueError(
            "rate window must be a positive Prometheus duration such as 1m or 5m"
        )
    scale = {"s": 1, "m": 60, "h": 3_600, "d": 86_400}[window[-1]]
    window_seconds = int(window[:-1]) * scale
    health_urls = {
        "collector": f"{COLLECTOR_URL}/",
        "tempo": f"{TEMPO_URL}/ready",
        "victoria_metrics": f"{VICTORIA_URL}/-/healthy",
        "grafana": f"{GRAFANA_URL}/api/health",
    }
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window": {
            "analysis": window,
            "analysis_seconds": window_seconds,
            "rates_and_latency": rate_window,
        },
        "deployment": _config_summary(),
        "backends": {
            name: _safe_json_request(url) for name, url in health_urls.items()
        },
        "host": {
            "load_average": [round(item, 6) for item in os.getloadavg()],
            "uptime_seconds": _number(
                Path("/proc/uptime").read_text(encoding="utf-8").split()[0]
            ),
            "memory": _meminfo(),
            "telemetry_disk": _disk(),
            "systemd_services": _systemd_services(),
            "telemetry_containers": _container_stats(),
        },
        "retention": {"traces": "72h", "metrics": "14d"},
        "agent_hints": [
            "Compare p95/p99 latency by operation before reading individual traces.",
            "Use recent latency for repeating operations and cumulative_latency_since_process_start for one-off park, wake, build, or bootstrap operations that may not produce a rate delta.",
            "In longest_spans, a low cpu_wall_ratio indicates waiting or I/O; a ratio near 1 indicates CPU work on that thread.",
            "Parent spans include child time; use leaf and phase spans to identify the actual bottleneck.",
            "For asyncio services, thread CPU is directional because unrelated coroutines share the event-loop thread.",
        ],
    }
    try:
        report["metrics"] = _metrics_snapshot(window, rate_window)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, RuntimeError) as exc:
        report["metrics"] = {"error": f"{type(exc).__name__}: {exc}"}
    try:
        report["traces"] = _traces_snapshot(window_seconds, trace_limit)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, RuntimeError) as exc:
        report["traces"] = {"error": f"{type(exc).__name__}: {exc}"}
    report["findings"] = _findings(report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate an agent-readable UCloud observability snapshot"
    )
    parser.add_argument("--window", default="30m")
    parser.add_argument(
        "--rate-window",
        default="5m",
        help="recent Prometheus range for operation rates and latency quantiles",
    )
    parser.add_argument("--trace-limit", type=int, default=5, choices=range(0, 21))
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args(argv)
    try:
        report = create_report(
            args.window,
            rate_window=args.rate_window,
            trace_limit=args.trace_limit,
        )
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        parser.error(str(exc))
    json.dump(
        report,
        sys.stdout,
        indent=None if args.compact else 2,
        sort_keys=True,
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
