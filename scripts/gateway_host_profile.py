#!/usr/bin/env python3
"""Read-only Linux gateway CPU attribution. Export counters, never process arguments.

Sample on the gateway; analyze the JSONL on any host. Cgroups are independent
attribution views, not quantities to sum: parent and child groups can overlap.
"""

import argparse
from datetime import datetime, timezone
import fnmatch
import json
import math
import os
from pathlib import Path
import signal
import statistics
import subprocess
import time


CPU_FIELDS = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")
SERVICE_NAMES = ("ucloud-*.service", "postgresql*.service", "postgres*.service",
                 "nginx.service", "docker.service", "containerd.service")


def read_text(path):
    try:
        return path.read_text()
    except (OSError, UnicodeError):
        return ""


def pairs(path):
    result = {}
    for line in read_text(path).splitlines():
        fields = line.split()
        if len(fields) == 2:
            try:
                result[fields[0]] = int(fields[1])
            except ValueError:
                pass
    return result


def pressure(path):
    result = {}
    for line in read_text(path).splitlines():
        fields = line.split()
        for field in fields[1:]:
            key, _, value = field.partition("=")
            try:
                result[fields[0] + "_" + key] = float(value)
            except ValueError:
                pass
    return result


def process_stat(text):
    """Parse comm containing spaces or parentheses without reading cmdline."""
    left, right = text.index("("), text.rindex(")")
    fields = text[right + 2:].split()
    return {"name": text[left + 1:right], "start_ticks": int(fields[19]),
            "user_ticks": int(fields[11]), "system_ticks": int(fields[12]),
            "threads": int(fields[17]), "rss_pages": int(fields[21])}


def process_snapshot(proc):
    result = {}
    for directory in proc.iterdir():
        if not directory.name.isdigit():
            continue
        try:
            row = process_stat(read_text(directory / "stat"))
            row["cgroup"] = next((line[3:] for line in read_text(directory / "cgroup").splitlines()
                                  if line.startswith("0::")), None)
            sched = read_text(directory / "schedstat").split()
            if len(sched) >= 2:
                row["scheduler_wait_ns"] = int(sched[1])
            result[directory.name] = row
        except (ValueError, IndexError, OSError):
            continue  # A process may exit between proc reads.
    return result


def container_groups(proc):
    """Resolve only selected container names and PIDs; no Docker config exported."""
    result = {}
    try:
        names = subprocess.run(["docker", "ps", "--format", "{{.Names}}"],
                               capture_output=True, text=True, timeout=3, check=True).stdout.splitlines()
        for name in names:
            if "registry" not in name and "postgres" not in name:
                continue
            pid = subprocess.run(["docker", "inspect", "--format", "{{.State.Pid}}", name],
                                 capture_output=True, text=True, timeout=3, check=True).stdout.strip()
            for line in read_text(proc / pid / "cgroup").splitlines():
                if line.startswith("0::"):
                    result["container:" + name] = line[3:]
    except (OSError, subprocess.SubprocessError):
        pass
    return result


def discover_groups(cgroup_root, containers):
    result = dict(containers)
    pending = [cgroup_root / "system.slice"]
    while pending:
        parent = pending.pop()
        try:
            children = list(parent.iterdir())
        except OSError:
            continue
        for path in children:
            # Instances such as PostgreSQL live in system-postgresql.slice.
            # Traverse host slices only; do not recurse into delegated service
            # or container trees and accidentally count their units twice.
            if path.is_symlink() or not path.is_dir():
                continue
            if path.name.endswith(".slice"):
                pending.append(path)
            elif any(fnmatch.fnmatchcase(path.name, pattern) for pattern in SERVICE_NAMES):
                relative = "/" + str(path.relative_to(cgroup_root))
                label = path.name if path.name not in result else relative
                result[label] = relative
    return result


def cgroup_snapshot(cgroup_root, groups):
    result = {}
    for name, relative in groups.items():
        path = cgroup_root / relative.lstrip("/")
        try:
            row = {"path": relative, "inode": path.stat().st_ino, "cpu": pairs(path / "cpu.stat"),
                   "cpu_pressure": pressure(path / "cpu.pressure")}
            for filename in ("memory.current", "memory.peak"):
                raw = read_text(path / filename).strip()
                if raw.isdigit():
                    row[filename.replace(".", "_") + "_bytes"] = int(raw)
            result[name] = row
        except OSError:
            continue
    return result


def protocol_counters(path, protocol, selected):
    lines = read_text(path).splitlines()
    for header, values in zip(lines, lines[1:]):
        keys, numbers = header.split(), values.split()
        if not keys or not numbers or keys[0] != protocol + ":" or numbers[0] != keys[0]:
            continue
        result = {}
        for key, value in zip(keys[1:], numbers[1:]):
            if key in selected:
                try:
                    result[key] = int(value)
                except ValueError:
                    pass
        if result:
            return result
    return {}


def network_snapshot(proc):
    interfaces = {}
    fields = {"rx_bytes": 0, "rx_packets": 1, "rx_errors": 2, "rx_drops": 3,
              "tx_bytes": 8, "tx_packets": 9, "tx_errors": 10, "tx_drops": 11}
    for line in read_text(proc / "net/dev").splitlines():
        name, separator, raw = line.partition(":")
        if not separator:
            continue
        values = raw.split()
        try:
            interfaces[name.strip()] = {field: int(values[index]) for field, index in fields.items()}
        except (ValueError, IndexError):
            continue
    softnet = {}
    for line in read_text(proc / "net/softnet_stat").splitlines():
        values = line.split()
        if len(values) < 3:
            continue
        try:
            row = {name: int(value, 16) for name, value in
                   zip(("processed", "dropped", "time_squeeze"), values[:3])}
        except ValueError:
            continue
        for name, value in row.items():
            softnet[name] = softnet.get(name, 0) + value
    conntrack = {}
    for name in ("count", "max"):
        value = read_text(proc / "sys/net/netfilter" / ("nf_conntrack_" + name)).strip()
        if value.isdigit():
            conntrack[name] = int(value)
    return {"interfaces": interfaces, "softnet": softnet, "conntrack": conntrack,
            "tcp": protocol_counters(proc / "net/snmp", "Tcp", {"RetransSegs", "InSegs", "OutSegs"}),
            "tcp_ext": protocol_counters(proc / "net/netstat", "TcpExt", {"ListenDrops", "ListenOverflows"})}


def host_snapshot(proc):
    stat = read_text(proc / "stat").splitlines()
    cpu = next((line.split()[1:] for line in stat if line.startswith("cpu ")), [])
    disks = {}
    for line in read_text(proc / "diskstats").splitlines():
        fields = line.split()
        if len(fields) < 14 or fields[2].startswith(("loop", "ram")):
            continue
        disks[fields[2]] = {"reads": int(fields[3]), "read_bytes": int(fields[5]) * 512,
                            "read_ms": int(fields[6]), "writes": int(fields[7]),
                            "write_bytes": int(fields[9]) * 512, "write_ms": int(fields[10]),
                            "inflight": int(fields[11]), "busy_ms": int(fields[12]),
                            "weighted_io_ms": int(fields[13])}
    return {"cpu_ticks": dict(zip(CPU_FIELDS, map(int, cpu))), "disks": disks,
            "online_cpu_count": sum(line.split()[0][3:].isdigit() for line in stat if line.startswith("cpu")),
            "network": network_snapshot(proc),
            "cpu_pressure": pressure(proc / "pressure/cpu"),
            "memory_pressure": pressure(proc / "pressure/memory"),
            "io_pressure": pressure(proc / "pressure/io"),
            "loadavg": read_text(proc / "loadavg").split()[:3]}


def sample(args):
    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    started, containers, discovery_due = time.monotonic(), {}, 0.0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Refuse to overwrite a previous measurement.
    with args.output.open("x", encoding="utf-8") as output:
        output.write(json.dumps({"type": "metadata", "version": 1, "clock_ticks": os.sysconf("SC_CLK_TCK"),
                                 "page_bytes": os.sysconf("SC_PAGE_SIZE"), "cpu_count": os.cpu_count(),
                                 "interval_seconds": args.interval,
                                 "note": "Counters only; cgroup views may overlap; exited short processes are not individually attributed."}) + "\n")
        while not stopping and time.monotonic() - started <= args.duration:
            before = time.monotonic()
            if before >= discovery_due:
                containers = container_groups(args.proc_root)
                discovery_due = before + 30
            row = {"type": "sample", "unix_seconds": time.time(), "monotonic_seconds": time.monotonic(),
                   "phase": read_text(args.phase_file).strip()[:80] if args.phase_file else "unlabeled",
                   "host": host_snapshot(args.proc_root),
                   "cgroups": cgroup_snapshot(args.cgroup_root, discover_groups(args.cgroup_root, containers)),
                   "processes": process_snapshot(args.proc_root)}
            row["collection_seconds"] = time.monotonic() - before
            output.write(json.dumps(row, separators=(",", ":")) + "\n")
            output.flush()
            # Short slices keep SIGTERM responsive even with a long interval.
            due = before + args.interval
            while not stopping and time.monotonic() < due:
                time.sleep(min(.1, max(0, due - time.monotonic())))


def distribution(values):
    values = sorted(values)
    return {"count": len(values), "mean": statistics.mean(values) if values else None,
            "p95": values[max(0, math.ceil(.95 * len(values)) - 1)] if values else None,
            "max": max(values) if values else None}


def delta(before, after, key):
    if key not in before or key not in after or after[key] < before[key]:
        return None
    return after[key] - before[key]


def interval(before, after, ticks):
    seconds = after["monotonic_seconds"] - before["monotonic_seconds"]
    if seconds <= 0:
        return None
    values = {}

    def add(key, value):
        if value is not None:
            values[key] = value / seconds

    old, new = before["host"], after["host"]
    busy = []
    for field in CPU_FIELDS:
        value = delta(old["cpu_ticks"], new["cpu_ticks"], field)
        add("host/" + field + "_cores", None if value is None else value / ticks)
        if field not in ("idle", "iowait", "steal") and value is not None:
            busy.append(value / ticks)
    if len(busy) == 5:
        add("host/busy_cores", sum(busy))
    for kind in ("cpu", "memory", "io"):
        for level in ("some", "full"):
            value = delta(old[kind + "_pressure"], new[kind + "_pressure"], level + "_total")
            add("host/" + kind + "_" + level + "_pressure_fraction", None if value is None else value / 1e6)
    old_net, new_net = old.get("network", {}), new.get("network", {})
    for name, current in new_net.get("interfaces", {}).items():
        previous = old_net.get("interfaces", {}).get(name, {})
        for field in current:
            add("network/" + name + "/" + field + "_per_second", delta(previous, current, field))
    for group in ("softnet", "tcp", "tcp_ext"):
        previous, current = old_net.get(group, {}), new_net.get(group, {})
        for field in current:
            add("network/" + group + "/" + field + "_per_second", delta(previous, current, field))
    for field, value in new_net.get("conntrack", {}).items():
        values["network/conntrack/" + field] = value
    if new_net.get("conntrack", {}).get("max", 0) > 0 and "count" in new_net["conntrack"]:
        values["network/conntrack/utilization_fraction"] = new_net["conntrack"]["count"] / new_net["conntrack"]["max"]
    if "online_cpu_count" in new:
        values["host/online_cpu_count"] = new["online_cpu_count"]
    for name, group in after.get("cgroups", {}).items():
        previous = before.get("cgroups", {}).get(name)
        if previous is None or previous.get("inode") != group.get("inode"):
            continue
        for source, suffix in (("usage_usec", "cores"), ("user_usec", "user_cores"),
                               ("system_usec", "system_cores"), ("throttled_usec", "throttled_seconds_per_second")):
            value = delta(previous["cpu"], group["cpu"], source)
            add("cgroup/" + name + "/" + suffix, None if value is None else value / 1e6)
    for pid, process in after.get("processes", {}).items():
        previous = before.get("processes", {}).get(pid)
        if previous is None or previous["start_ticks"] != process["start_ticks"]:
            continue
        user = delta(previous, process, "user_ticks")
        system = delta(previous, process, "system_ticks")
        if user is not None and system is not None:
            add("process/" + pid + ":" + str(process["start_ticks"]) + "/cores", (user + system) / ticks)
            group = process.get("cgroup")
            if group and group == previous.get("cgroup"):
                key = "sampled_process_cgroup/" + group.lstrip("/") + "/cores"
                values[key] = values.get(key, 0) + (user + system) / ticks / seconds
    for name, disk in new["disks"].items():
        previous = old["disks"].get(name)
        if previous is None:
            continue
        for field in ("read_bytes", "write_bytes", "reads", "writes", "read_ms", "write_ms", "busy_ms", "weighted_io_ms"):
            add("disk/" + name + "/" + field + "_per_second", delta(previous, disk, field))
    return {"seconds": seconds, "phase": after.get("phase", "unlabeled"),
            "unix_seconds": after["unix_seconds"], "values": values}


def summarize_intervals(rows):
    values = {}
    for row in rows:
        for key, value in row["values"].items():
            values.setdefault(key, []).append((row["seconds"], value))
    result = {}
    for key, observations in values.items():
        covered = sum(seconds for seconds, _ in observations)
        integral = sum(seconds * value for seconds, value in observations)
        result[key] = {**distribution([value for _, value in observations]),
                       "mean": integral / covered, "covered_seconds": covered}
        if key.endswith("/cores") or key.endswith("_cores"):
            result[key]["cpu_seconds"] = integral
        elif key.endswith("_per_second"):
            result[key]["counter_delta"] = integral
    return result


def phase_summaries(rows):
    return {phase: {key: value for key, value in summarize_intervals(
        [row for row in rows if row["phase"] == phase]).items() if not key.startswith("process/")}
            for phase in sorted({row["phase"] for row in rows})}


def harness_phases(intervals, events):
    """Join only event names/times, never guest payloads or driver credentials."""
    boundaries = []
    completed = []
    correct = False
    labels = {"capacity_requested": "during_creation", "all_agents_ready": "all_agents_started",
              "benchmark_finished": "after_run"}
    for event in events:
        try:
            timestamp = datetime.fromisoformat(event["at"].replace("Z", "+00:00")).timestamp()
        except (KeyError, TypeError, ValueError, AttributeError):
            continue
        name = event.get("event")
        if name in labels:
            boundaries.append((timestamp, labels[name], name))
        if name == "scenario_completed":
            completed.append(timestamp)
        if name == "benchmark_finished":
            correct = event.get("correct") is True
    # A failed/partial run cannot identify the cleanup start from the last
    # successful scenario; leave that interval in the preceding load phase.
    if correct and completed:
        boundaries.append((max(completed), "cleanup", "last_scenario_completed"))
    boundaries.sort()
    rows = []
    for row in intervals:
        phase = "before_run"
        for timestamp, label, _ in boundaries:
            if timestamp <= row["unix_seconds"]:
                phase = label
        rows.append({**row, "phase": phase})
    return (phase_summaries(rows) if boundaries else {},
            [{"event": name, "unix_seconds": timestamp} for timestamp, _, name in boundaries])


def analyze_records(records, events=()):
    metadata = next(row for row in records if row.get("type") == "metadata")
    samples = [row for row in records if row.get("type") == "sample"]
    intervals = [value for old, new in zip(samples, samples[1:])
                 if (value := interval(old, new, metadata["clock_ticks"])) is not None]
    summary = summarize_intervals(intervals)
    process_names = {}
    for sample_row in samples:
        for pid, process in sample_row.get("processes", {}).items():
            process_names[pid + ":" + str(process["start_ticks"])] = {
                "name": process["name"], "cgroup": process.get("cgroup")}
    hottest = []
    for key, value in summary.items():
        if key.startswith("process/"):
            identity = key.split("/")[1]
            hottest.append({"identity": identity, **process_names[identity], **value})
    hottest.sort(key=lambda row: row.get("cpu_seconds", 0), reverse=True)
    joined_phases, marks = harness_phases(intervals, events)
    return {"metadata": metadata, "sample_count": len(samples), "interval_count": len(intervals),
            "started_at": datetime.fromtimestamp(samples[0]["unix_seconds"], timezone.utc).isoformat() if samples else None,
            "finished_at": datetime.fromtimestamp(samples[-1]["unix_seconds"], timezone.utc).isoformat() if samples else None,
            "collection_seconds": distribution([row.get("collection_seconds", 0) for row in samples]),
            "summary": {key: value for key, value in summary.items() if not key.startswith("process/")},
            "hottest_processes": hottest[:30], "harness_phase_timestamps": marks,
            "phases": phase_summaries(intervals), "harness_phases": joined_phases,
            "limitations": ["Cgroup rows may overlap and must not be summed without checking their paths.",
                            "Short-lived processes between samples are covered by host/cgroup CPU but absent from per-process attribution.",
                            "sampled_process_cgroup totals are reconstructed from observed processes, not authoritative cgroup counters; use only as a fallback.",
                            "Disk rows may include both whole devices and partitions; do not sum these.",
                            "Interface counters can count the same forwarded packet on several interfaces; do not sum as unique traffic.",
                            "Phase joins use each interval's final timestamp and require synchronized driver/gateway clocks.",
                            "p95 is the nearest-rank sample percentile; mean is weighted by elapsed sample time."]}


def read_jsonl(path):
    rows = []
    for line in path.read_text().splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # Accept an interrupted final write or non-JSON driver log line.
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    sampler = commands.add_parser("sample")
    sampler.add_argument("--output", required=True, type=Path)
    sampler.add_argument("--duration", type=float, default=1800)
    sampler.add_argument("--interval", type=float, default=1)
    sampler.add_argument("--phase-file", type=Path)
    sampler.add_argument("--proc-root", type=Path, default=Path("/proc"))
    sampler.add_argument("--cgroup-root", type=Path, default=Path("/sys/fs/cgroup"))
    analyzer = commands.add_parser("analyze")
    analyzer.add_argument("input", type=Path)
    analyzer.add_argument("--events", type=Path)
    analyzer.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "sample":
        if not math.isfinite(args.duration) or not math.isfinite(args.interval) or args.duration <= 0 or args.interval <= 0:
            parser.error("duration and interval must be finite and positive")
        sample(args)
    else:
        result = analyze_records(read_jsonl(args.input), read_jsonl(args.events) if args.events else ())
        args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
