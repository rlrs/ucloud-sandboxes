#!/usr/bin/env python3
"""Interpret a local build_load_report.json and its retained raw host telemetry.

Run build_load_report.py first after refreshing raw files, then this script with
the same --root. No network access or production changes are performed. Source
fingerprints and coverage are retained; no running-sandbox capacity is inferred.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import shlex
import tempfile


MIB, GIB = 2**20, 2**30
GROUPS = ("gateway", "registry", "benchmark_driver", "postgres", "nginx", "relay",
          "autoscaler", "placement", "node_agent", "dockerd", "buildkit", "erofs")
LIMITATIONS = [
    "These results describe the sampled image-build pipeline. They do not qualify 500 or 1,000 running agent sandboxes, model-relay traffic, or their mixed load.",
    "Batch averages include submission, queueing, execution and completion tails. Repeated fixture graphs can share BuildKit work. Execution overlap includes build, push and EROFS publication, not only CPU-heavy RUN instructions.",
    "The gateway-local SDK driver consumes host CPU. Process groups are exclusive, but short-lived processes and asynchronous counter reads limit exact attribution. Never subtract independently calculated percentiles.",
    "Disk busy_percent is deliberately excluded from all conclusions and tables because earlier qualification exposed unreliable busy_ms counter jumps. This run's raw counters and any flagged anomalies are retained in JSON. No disk-utilization ceiling is inferred.",
    "Disk bytes, request latency, queue depth, PSI and iowait are separate signals. Traffic includes buffering, writeback, metadata, cache exports and maintenance. Physical reads near zero can mean page-cache hits. Do not sum overlapping devices or network interfaces.",
    "Low gateway CPU does not establish why admission returned 503 or why a build queued. Correlate per-build phases before assigning a specific cause or raising concurrency.",
    "Telemetry means are time-weighted and p95 is nearest-rank over sampled intervals. API/client p95 uses linear interpolation. Only complete counter intervals inside each client window are included; short phases lose a larger fraction at their boundaries.",
    "Health probes originate on the gateway; SDK timing includes a gateway-local client. Neither is an external end-to-end capacity test. No OOM delta means none observed in covered intervals, not a complete kernel-log audit.",
]


def fingerprint(path):
    return {"bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def metric(summary, key, divisor=1):
    """Scale observations, preserving sample counts and interval duration."""
    return {name: value / divisor if name in {"mean", "min", "max", "p95"} else value
            for name, value in summary.get(key, {}).items() if name != "counter_delta"}


def source_info(path):
    """Inspect raw counters, without trusting previously derived busy rates."""
    previous, first, last, skipped = None, None, None, 0
    cpus, anomalies = set(), []
    with (gzip.open(path, "rt") if path.suffix == ".gz" else path.open()) as source:
        for line in source:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            if not isinstance(row, dict) or row.get("type") != "sample":
                continue
            first = first or row
            last = row
            cpus.add(row["raw"]["online_cpus"])
            if previous is not None and row["raw"].get("boot_id") == previous["raw"].get("boot_id"):
                seconds = row["monotonic_seconds"] - previous["monotonic_seconds"]
                for device, disk in row["raw"]["disks"].items():
                    old = previous["raw"]["disks"].get(device)
                    if old is None or seconds <= 0:
                        continue
                    rate = (disk["busy_ms"] - old["busy_ms"]) / (seconds * 10)
                    if rate > 100:
                        anomalies.append({"at": row["at"], "device": device,
                                          "derived_busy_percent": rate, "seconds": seconds,
                                          "previous_busy_ms": old["busy_ms"],
                                          "current_busy_ms": disk["busy_ms"]})
            previous = row
    return {**fingerprint(path), "first_sample": first["at"] if first else None,
            "last_sample": last["at"] if last else None, "online_cpus": sorted(cpus),
            "memory_total_gib": first["raw"]["memory_bytes"].get("MemTotal", 0) / GIB if first else None,
            "malformed_lines": skipped, "excluded_disk_busy_counter_anomalies": anomalies}


def host_summary(host, gateway_label, gateway_disk, builder_disk):
    if host.get("error"):
        return {"source": host["source"], "error": host["error"]}
    summary = host["summary"]
    label = host.get("metadata", {}).get("label") or Path(host["source"]).stem
    device = gateway_disk if label == gateway_label else builder_disk
    disk = {}
    for direction in ("read", "write"):
        key = f"disk/{device}/{direction}_bytes_per_second"
        delta = summary.get(key, {}).get("counter_delta")
        disk[direction] = {"mib_per_second": metric(summary, key, MIB),
                           "total_gib": delta / GIB if delta is not None else None}
    return {"label": label, "source": host["source"], "coverage_fraction": host["coverage_fraction"],
            "covered_seconds": host["covered_seconds"], "started_at": host["started_at"],
            "finished_at": host["finished_at"], "counter_resets": host["counter_resets"],
            "host_cpu_cores": metric(summary, "cpu/busy_cores"),
            "iowait_cores": metric(summary, "cpu/iowait_cores"),
            "process_cpu_cores": {group: metric(summary, f"process/{group}/cpu_cores") for group in GROUPS},
            "memory_available_gib": metric(summary, "memory/MemAvailable_bytes", GIB),
            "pressure_percent": {resource: metric(summary, f"pressure/{resource}/some_percent")
                                 for resource in ("cpu", "memory", "io")},
            "oom_kills": summary.get("vmstat/oom_kill_per_second", {}).get("counter_delta"),
            "swap_pages_in": summary.get("vmstat/pswpin_per_second", {}).get("counter_delta"),
            "swap_pages_out": summary.get("vmstat/pswpout_per_second", {}).get("counter_delta"),
            "disk": {"device": device, **disk, "await_ms": metric(summary, f"disk/{device}/await_ms"),
                     "queue_depth": metric(summary, f"disk/{device}/average_queue_depth")},
            "network_drop_deltas": {key: value["counter_delta"] for key, value in summary.items()
                                    if key.startswith("network/") and key.endswith("drops_per_second")},
            "health": host["health"]}


def outside_phase_window(host, phase, source):
    """A replaced pool is not missing telemetry for times before/after its life."""
    label = host.get("label", "")
    if (not label.startswith("builder-") or host.get("coverage_fraction") != 0
            or label.removeprefix("builder-") in phase["owners"]):
        return False
    window = phase.get("window") or {}
    stamps = [source.get("first_sample"), source.get("last_sample"),
              window.get("started_at"), window.get("finished_at")]
    if not all(stamps):
        return False
    first, last, start, finish = [datetime.fromisoformat(value.replace("Z", "+00:00")) for value in stamps]
    return last < start or first > finish


def analyze(report, args):
    sources, phases = {}, []
    for phase in report["phases"]:
        hosts = []
        for host in phase["telemetry"]:
            source = Path(host["source"])
            if str(source) not in sources:
                try:
                    sources[str(source)] = source_info(source)
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    sources[str(source)] = {"error": f"{type(exc).__name__}: {exc}"}
            compact = host_summary(host, args.gateway_label, args.gateway_disk, args.builder_disk)
            compact["relevance"] = ("outside_phase_window" if outside_phase_window(compact, phase, sources[str(source)])
                                    else "included")
            hosts.append(compact)
        phases.append({key: phase[key] for key in ("phase", "window", "cases", "statuses", "owners", "http",
                                                  "measurements", "overlap", "batch_wall_seconds")} | {"hosts": hosts})
    note = args.root / "build-triggered-worker.json"
    return {"schema": 1, "generated_at": datetime.now(timezone.utc).isoformat(), "root": str(args.root),
            "source_report": str(args.report.resolve()), "report_fingerprint": fingerprint(args.report),
            "gateway_label": args.gateway_label, "gateway_disk": args.gateway_disk,
            "builder_disk": args.builder_disk, "sources": sources, "phases": phases,
            "run_specific_evidence": [str(note.resolve())] if note.exists() else [],
            "limitations": LIMITATIONS}


def fmt(value, digits=3):
    return f"{value:.{digits}f}" if isinstance(value, (int, float)) else "missing"


def triple(values):
    return " / ".join(fmt(values.get(key)) for key in ("mean", "p95", "max")) if values else "missing"


def table_row(values):
    return "| " + " | ".join(str(value).replace("|", "\\|").replace("\n", " ") for value in values) + " |"


def minimum(hosts, key):
    values = [host.get(key, {}).get("min") for host in hosts]
    return min((value for value in values if value is not None), default=None)


def markdown(report):
    phases = report["phases"]
    gateway_label = report["gateway_label"]
    gateways = [(phase, host) for phase in phases for host in phase["hosts"]
                if host.get("label") == gateway_label]
    builders = [(phase, host) for phase in phases for host in phase["hosts"]
                if host.get("label") != gateway_label and not host.get("error")
                and host.get("relevance") != "outside_phase_window"]
    success = sum(phase["statuses"].get("succeeded", 0) for phase in phases)
    cases = sum(phase["cases"] for phase in phases)
    api_means = [host["process_cpu_cores"]["gateway"].get("mean") for _, host in gateways]
    api_means = [value for value in api_means if value is not None]
    lines = ["# Build-load host findings", "",
             f"Observed {success}/{cases} successful builds across {len(phases)} phases. These are image-build results; they do not qualify 500 or 1,000 concurrently running agent sandboxes.", ""]
    if api_means:
        lines += [f"Gateway API CPU averaged {min(api_means):.3f}–{max(api_means):.3f} occupied cores by phase. Use the registry I/O and builder pressure below to assess build costs; gateway CPU alone does not explain admission delays.", ""]
    lines += ["| Phase | Success / cases | Executing peak | Worker queue p95 s | Submit 503s | Poll-header p95 ms | Submit-header p95 s |",
              "|---|---:|---:|---:|---:|---:|---:|"]
    for phase in phases:
        http = phase["http"]
        latency = http["time_to_headers_seconds"]
        poll = latency.get("poll", {}).get("p95")
        submit503 = http["by_category"].get("submit", {}).get("503", 0)
        lines.append(table_row([phase["phase"], f"{phase['statuses'].get('succeeded', 0)}/{phase['cases']}",
                                phase["overlap"]["executing"]["peak"], fmt(phase["measurements"]["queue_seconds"]["p95"]),
                                submit503, fmt(poll * 1000 if poll is not None else None),
                                fmt(latency.get("submit", {}).get("p95"))]))
    lines += ["", "CPU is occupied cores; each cell is mean / p95 / maximum. Counts of online CPUs are in JSON. The SDK driver runs on the gateway and has its own attribution.", "",
              "| Phase | Gateway coverage | Host CPU | API CPU | Registry CPU | SDK driver CPU | Health failures / probes; p95 ms |",
              "|---|---:|---:|---:|---:|---:|---:|"]
    for phase, host in gateways:
        health = host["health"]
        lines.append(table_row([phase["phase"], f"{host['coverage_fraction']:.1%}", triple(host["host_cpu_cores"]),
                                *[triple(host["process_cpu_cores"][key]) for key in ("gateway", "registry", "benchmark_driver")],
                                f"{health['failures']}/{health['probes']}; {fmt(health['latency_p95_ms'], 2)}"]))
    probes = sum(host["health"]["probes"] for _, host in gateways)
    failures = sum(host["health"]["failures"] for _, host in gateways)
    lines += ["", f"Observed {failures} failed health probes among {probes} samples within phase windows. Submission timing includes admission/context transfer; it is not pure HTTP handler overhead.", "",
              f"The gateway disk below is configured as `{report['gateway_disk']}` (the registry volume in this run). Disk busy percentage remains excluded because earlier qualification exposed unreliable counter jumps; it is not used as capacity evidence. This run's anomaly records are retained in JSON.", "",
              "| Phase | Write MiB/s mean / p95 / max | Written GiB | I/O-weighted await ms | Queue mean / p95 / max | Host I/O PSI mean / p95 % | Iowait cores mean / p95 / max |",
              "|---|---:|---:|---:|---:|---:|---:|"]
    for phase, host in gateways:
        disk, psi = host["disk"], host["pressure_percent"]["io"]
        lines.append(table_row([phase["phase"], triple(disk["write"]["mib_per_second"]), fmt(disk["write"]["total_gib"]),
                                fmt(disk["await_ms"].get("io_weighted_mean"), 2), triple(disk["queue_depth"]),
                                f"{fmt(psi.get('mean'), 2)} / {fmt(psi.get('p95'), 2)}", triple(host["iowait_cores"])]))
    lines += ["", "Request latency, queueing, iowait and PSI together show periods of storage pressure. They do not prove every slow build waited on the registry or establish a sustained disk-throughput ceiling. Writeback can continue beyond a phase.", "",
              "| Phase / builder | Coverage | Host CPU mean / p95 / max | CPU PSI mean / p95 / max % | Minimum available GiB | I/O PSI mean / p95 / max % | OOM delta |",
              "|---|---:|---:|---:|---:|---:|---:|"]
    for phase, host in builders:
        lines.append(table_row([phase["phase"] + " / " + host["label"].removeprefix("builder-"),
                                f"{host['coverage_fraction']:.1%}", triple(host["host_cpu_cores"]),
                                triple(host["pressure_percent"]["cpu"]), fmt(host["memory_available_gib"].get("min"), 2),
                                triple(host["pressure_percent"]["io"]), fmt(host["oom_kills"], 0)]))
    all_hosts = [host for _, host in gateways + builders]
    lines += ["", f"Minimum sampled available memory: gateway {fmt(minimum([host for _, host in gateways], 'memory_available_gib'), 2)} GiB; builders {fmt(minimum([host for _, host in builders], 'memory_available_gib'), 2)} GiB."]
    complete = all_hosts and all(host[key] is not None for host in all_hosts for key in ("oom_kills", "swap_pages_in", "swap_pages_out"))
    if complete and all(host[key] == 0 for host in all_hosts for key in ("oom_kills", "swap_pages_in", "swap_pages_out")):
        lines.append("No OOM kills or swap activity were observed in covered phase intervals.")
    memory_psi = [host["pressure_percent"]["memory"].get("mean") for host in all_hosts]
    memory_psi = [value for value in memory_psi if value is not None]
    if memory_psi:
        lines.append(f"Largest phase/host mean memory PSI was {max(memory_psi):.4f}%; transient maxima are retained in JSON.")
    lines += ["", "Work and CPU pressure vary by builder and recipe. Low batch averages do not justify raising all concurrency limits: inspect the busiest execution intervals and queue/admission behavior."]
    cpu_counts = [report["sources"].get(host["source"], {}).get("online_cpus", []) for _, host in gateways]
    cpu_counts = [count for counts in cpu_counts for count in counts]
    has_disk_pressure = any(host["disk"]["await_ms"].get("io_weighted_mean", 0) >= 20
                            and host["disk"]["queue_depth"].get("mean", 0) >= 1 for _, host in gateways)
    if api_means and cpu_counts and max(api_means) < min(cpu_counts) * .1 and has_disk_pressure:
        lines.append("Low measured API CPU alongside registry I/O pressure supports reducing avoidable registry transfer/publication work before changing the gateway implementation for this build workload.")
    lines.append("")
    if report["run_specific_evidence"]:
        lines += ["Run-specific confounder: cold builds caused a sandbox worker to be provisioned while sandbox demand remained zero. Warm phases inherited that node; the autoscaler later stopped it. See `build-triggered-worker.json`; whole-host cold/warm differences are not entirely cache effects.", ""]
    warnings = []
    for phase in phases:
        for host in phase["hosts"]:
            if host.get("relevance") == "outside_phase_window":
                continue
            if host.get("error"):
                warnings.append(f"{phase['phase']}: {host['error']}")
            elif host["coverage_fraction"] < .9:
                warnings.append(f"{phase['phase']} / {host['label']}: only {host['coverage_fraction']:.1%} of the client window has complete telemetry intervals.")
    if warnings:
        lines += ["Coverage warnings:", "", *["- " + warning for warning in warnings], ""]
    if any(host.get("relevance") == "outside_phase_window" for phase in phases for host in phase["hosts"]):
        lines += ["Builders whose retained samples lie entirely outside a phase and which owned none of its builds are omitted from that phase's tables. JSON marks them `outside_phase_window`; no missing-data conclusion is drawn for a replaced pool.", ""]
    lines += ["Limits:", "", *["- " + limit for limit in report["limitations"]], "",
              "Reproduce after copying complete raw telemetry and phase summaries:", "", "```sh",
              "python3 scripts/build_load_report.py --root " + shlex.quote(report["root"]),
              "python3 scripts/build_load_host_analysis.py --root " + shlex.quote(report["root"]), "```", ""]
    return "\n".join(lines)


def self_test():
    # Unit conversion must never scale a sample count/duration or imply that an absent counter is zero.
    summary = {"disk/sdb/write_bytes_per_second": {"mean": 2*MIB, "p95": 3*MIB, "max": 4*MIB,
                                                 "samples": 5, "covered_seconds": 10, "counter_delta": 20*MIB}}
    result = metric(summary, "disk/sdb/write_bytes_per_second", MIB)
    assert result == {"mean": 2, "p95": 3, "max": 4, "samples": 5, "covered_seconds": 10}
    assert metric(summary, "missing") == {}
    host = {"label": "builder-1", "coverage_fraction": 0}
    phase = {"owners": {"2": 1}, "window": {"started_at": "2026-09-29T02:00:00Z",
                                            "finished_at": "2026-09-29T03:00:00Z"}}
    old_source = {"first_sample": "2026-09-29T00:00:00+00:00", "last_sample": "2026-09-29T01:00:00+00:00"}
    assert outside_phase_window(host, phase, old_source)
    phase["owners"]["1"] = 1
    assert not outside_phase_window(host, phase, old_source)  # A claimed owner with missing data must stay visible.
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "raw.jsonl"
        rows = [{"type": "sample", "at": f"sample-{i}", "monotonic_seconds": 10+2*i,
                 "raw": {"boot_id": "test", "online_cpus": 4, "memory_bytes": {"MemTotal": 16*GIB},
                         "disks": {"sdb": {"busy_ms": busy}}},
                 "interval": {"values": {"disk/sdb/busy_percent": 0}}}
                for i, busy in enumerate((100, 20100))]
        path.write_text("\n".join(json.dumps(row) for row in rows) + "\n{truncated\n")
        info = source_info(path)
        assert info["malformed_lines"] == 1 and info["online_cpus"] == [4]
        assert info["excluded_disk_busy_counter_anomalies"][0]["derived_busy_percent"] == 1000
        archive = path.with_suffix(".jsonl.gz")
        with gzip.open(archive, "wt") as output:
            output.write(path.read_text())
        zipped = source_info(archive)
        assert zipped["excluded_disk_busy_counter_anomalies"] == info["excluded_disk_busy_counter_anomalies"]
    print("host analysis self-test passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--report", type=Path, help="Defaults to ROOT/build-load-report.json")
    parser.add_argument("--gateway-label", default="gateway")
    parser.add_argument("--gateway-disk", default="sdb")
    parser.add_argument("--builder-disk", default="sda")
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if args.root is None:
        parser.error("--root is required")
    args.report = args.report or args.root / "build-load-report.json"
    report = analyze(json.loads(args.report.read_text()), args)
    outputs = (args.json_output or args.root / "host-findings.json",
               args.markdown_output or args.root / "host-findings.md")
    inputs = {args.report.resolve(), *[Path(path).resolve() for path in report["sources"]],
              *[path.resolve() for path in args.root.glob("*/summary.json")]}
    if outputs[0].resolve() == outputs[1].resolve() or any(path.resolve() in inputs for path in outputs):
        parser.error("outputs must be distinct and cannot overwrite input artifacts")
    for path in outputs:
        path.parent.mkdir(parents=True, exist_ok=True)
    outputs[0].write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    outputs[1].write_text(markdown(report))
    print(json.dumps({"phases": len(report["phases"]), "json": str(outputs[0]), "markdown": str(outputs[1])}))


if __name__ == "__main__":
    main()
