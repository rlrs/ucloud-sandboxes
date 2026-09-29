#!/usr/bin/env python3
"""Bounded, read-only Linux build-load telemetry; standalone standard library.

Raw counters and interval rates share UTC/monotonic timestamps. Process
arguments, environments, HTTP bodies, and HTTP headers are never recorded.
Disk/interface rows are separate accounting views and must not be summed.
"""

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import signal
import sys
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


CPU_FIELDS = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")
BUSY_FIELDS = ("user", "nice", "system", "irq", "softirq")
MEMORY_FIELDS = set("MemTotal MemFree MemAvailable Buffers Cached SReclaimable Slab "
                    "AnonPages Shmem Dirty Writeback SwapTotal SwapFree PageTables".split())
GROUPS = ("gateway", "relay", "autoscaler", "placement", "node_agent", "benchmark_driver", "postgres",
          "nginx", "dockerd", "containerd", "buildkit", "registry", "erofs")
DEFAULT_DISK_PATHS = ("/", "/mnt/ucloud-registry", "/var/lib/ucloud-sandboxes/docker",
                      "/var/lib/ucloud-sandboxes/docker-xfs", "/var/lib/docker")
LIMITATIONS = [
    "Busy CPU excludes idle, iowait and steal; CPU units are occupied cores, not percentages.",
    "Process groups are exclusive, but RSS sums double-count shared pages. Short-lived exited processes can escape process CPU attribution; whole-host CPU still includes them.",
    "Disk rows include devices, partitions and loop devices; never sum overlapping layers. Disk sectors are always converted with 512 bytes/sector.",
    "Network interfaces can observe the same forwarded traffic; their byte rates are not additive unique traffic.",
    "GC/writeback can occur after a build. Correlate UTC windows with build phases and retain post-build samples.",
    "Summary windows include only complete counter intervals; partial boundary intervals are excluded. Means are time-weighted and p95 is nearest-rank over interval samples.",
    "Health probes run on the sampled host, validate TLS, do not follow redirects, and are not external end-to-end capacity tests.",
]


def text(path):
    try:
        return path.read_text()
    except (OSError, UnicodeError):
        return ""


def utc(timestamp=None):
    return datetime.fromtimestamp(time.time() if timestamp is None else timestamp, timezone.utc).isoformat()


def pressure(path):
    result = {}
    for line in text(path).splitlines():
        fields = line.split()
        if not fields or fields[0] not in {"some", "full"}:
            continue
        for item in fields[1:]:
            key, separator, value = item.partition("=")
            if separator and key in {"avg10", "avg60", "avg300", "total"}:
                try:
                    result[fields[0] + "_" + key] = int(value) if key == "total" else float(value)
                except ValueError:
                    pass
    return result


def process_stat(raw):
    left, right = raw.index("("), raw.rindex(")")
    fields = raw[right + 2:].split()
    return {"comm": raw[left + 1:right], "parent": int(fields[1]),
            "user_ticks": int(fields[11]), "system_ticks": int(fields[12]),
            "threads": int(fields[17]), "start_ticks": int(fields[19]),
            "rss_bytes": max(0, int(fields[21])) * os.sysconf("SC_PAGE_SIZE")}


def direct_group(row):
    name, cgroup = row["comm"], row["cgroup"]
    names = {"postgres": "postgres", "nginx": "nginx", "dockerd": "dockerd",
             "buildkitd": "buildkit", "buildkit-runc": "buildkit", "registry": "registry",
             "mkfs.erofs": "erofs", "fsck.erofs": "erofs", "dump.erofs": "erofs"}
    if name in names:
        return names[name]
    if name.startswith("containerd"):
        return "containerd"
    for unit, group in (("ucloud-sandbox-gateway", "gateway"), ("ucloud-model-relay", "relay"),
                        ("ucloud-sandbox-relay", "relay"),
                        ("ucloud-build-load-client", "benchmark_driver"),
                        ("ucloud-sandbox-autoscaler", "autoscaler"),
                        ("ucloud-sandbox-placement", "placement"),
                        ("ucloud-sandbox-node", "node_agent")):
        if any(part.startswith(unit) and part.endswith(".service") for part in cgroup.split("/")):
            return group
    return None


def processes(proc):
    rows = {}
    try:
        paths = list(proc.iterdir())
    except OSError:
        return rows
    for directory in paths:
        if not directory.name.isdigit():
            continue
        try:
            row = process_stat(text(directory / "stat"))
            row["cgroup"] = next((line.partition("::")[2] for line in text(directory / "cgroup").splitlines()
                                  if line.startswith("0::")), "")
            row["group"] = direct_group(row)
            rows[int(directory.name)] = row
        except (ValueError, IndexError, OSError):
            continue
    buildkit_groups = {row["cgroup"] for row in rows.values()
                       if row["comm"] == "buildkitd" and row["cgroup"] not in {"", "/"}}
    result = {}
    for pid, row in rows.items():
        if row["group"] is None and any(row["cgroup"] == path or row["cgroup"].startswith(path + "/")
                                        for path in buildkit_groups):
            row["group"] = "buildkit"
        if row["group"] is None:
            parent, visited = row["parent"], {pid}
            while parent in rows and parent not in visited:
                visited.add(parent)
                ancestor = rows[parent]
                if ancestor["group"]:
                    row["group"] = ancestor["group"]
                    break
                parent = ancestor["parent"]
        if row["group"]:
            # Export group labels, never arbitrary cgroup paths or argv.
            result[str(pid)] = {key: value for key, value in row.items()
                                if key not in {"comm", "cgroup", "parent"}}
    return result


def snapshot(proc, disk_paths):
    stat = text(proc / "stat").splitlines()
    cpu = next((line.split()[1:] for line in stat if line.startswith("cpu ")), [])
    raw = {"cpu_ticks": dict(zip(CPU_FIELDS, map(int, cpu[:8]))),
           "online_cpus": sum(bool(re.fullmatch(r"cpu\d+ .*", line)) for line in stat),
           "memory_bytes": {}, "pressure": {}, "disks": {}, "network": {},
           "vmstat": {}, "space": {}}
    uptime = text(proc / "uptime").split()
    raw["uptime_seconds"] = float(uptime[0]) if uptime else 0
    raw["boot_id"] = text(proc / "sys/kernel/random/boot_id").strip()
    for line in text(proc / "meminfo").splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[0].rstrip(":") in MEMORY_FIELDS:
            raw["memory_bytes"][fields[0].rstrip(":")] = int(fields[1]) * 1024
    for resource in ("cpu", "memory", "io"):
        raw["pressure"][resource] = pressure(proc / "pressure" / resource)
    for line in text(proc / "vmstat").splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] in {"pswpin", "pswpout", "pgmajfault", "oom_kill"}:
            raw["vmstat"][fields[0]] = int(fields[1])
    for line in text(proc / "diskstats").splitlines():
        fields = line.split()
        if len(fields) < 14 or fields[2].startswith("ram"):
            continue
        try:
            if fields[2].startswith(("loop", "nbd", "ublkb")) and not any(
                    int(fields[index]) for index in (3, 7, 11)):
                continue  # Hundreds of unused NBD devices need no exported rows.
            raw["disks"][fields[2]] = {
                "major": int(fields[0]), "minor": int(fields[1]),
                "reads": int(fields[3]), "read_bytes": int(fields[5]) * 512,
                "read_ms": int(fields[6]), "writes": int(fields[7]),
                "write_bytes": int(fields[9]) * 512, "write_ms": int(fields[10]),
                "inflight": int(fields[11]), "busy_ms": int(fields[12]),
                "weighted_io_ms": int(fields[13]),
            }
        except ValueError:
            continue
    for line in text(proc / "net/dev").splitlines():
        name, separator, values = line.partition(":")
        if not separator:
            continue
        fields = values.split()
        try:
            raw["network"][name.strip()] = {key: int(fields[index]) for key, index in
                (("rx_bytes", 0), ("rx_packets", 1), ("rx_errors", 2), ("rx_drops", 3),
                 ("tx_bytes", 8), ("tx_packets", 9), ("tx_errors", 10), ("tx_drops", 11))}
        except (ValueError, IndexError):
            continue
    for path in disk_paths:
        try:
            info = os.statvfs(path)
            raw["space"][path] = {"total_bytes": info.f_blocks * info.f_frsize,
                "free_bytes": info.f_bfree * info.f_frsize, "available_bytes": info.f_bavail * info.f_frsize,
                "inodes": info.f_files, "available_inodes": info.f_favail,
                "device": os.stat(path).st_dev}
        except OSError as exc:
            raw["space"][path] = {"error": type(exc).__name__}
    raw["processes"] = processes(proc)
    return raw


def difference(before, after, key):
    old, new = before.get(key), after.get(key)
    if type(old) not in (float, int) or type(new) not in (float, int) or new < old:
        return None
    return new - old


def derive(before, after, ticks):
    seconds = after["monotonic_seconds"] - before["monotonic_seconds"]
    old, new = before["raw"], after["raw"]
    if seconds <= 0 or old.get("boot_id") != new.get("boot_id"):
        return None
    values = {"host/online_cpus": new["online_cpus"]}
    resets = []

    def rate(key, previous, current, field, factor=1):
        value = difference(previous, current, field)
        if value is not None:
            values[key] = value * factor / seconds
        elif field in previous and field in current:
            resets.append(key)
        return value

    for field in CPU_FIELDS:
        rate("cpu/" + field + "_cores", old["cpu_ticks"], new["cpu_ticks"], field, 1 / ticks)
    if all("cpu/" + field + "_cores" in values for field in BUSY_FIELDS):
        values["cpu/busy_cores"] = sum(values["cpu/" + field + "_cores"] for field in BUSY_FIELDS)
    for resource, current in new["pressure"].items():
        previous = old["pressure"].get(resource, {})
        for level in ("some", "full"):
            rate(f"pressure/{resource}/{level}_percent", previous, current, level + "_total", 100 / 1e6)
    for field in new["vmstat"]:
        rate("vmstat/" + field + "_per_second", old["vmstat"], new["vmstat"], field)
    for name, current in new["network"].items():
        previous = old["network"].get(name, {})
        for field in current:
            rate(f"network/{name}/{field}_per_second", previous, current, field)
    for name, current in new["disks"].items():
        previous = old["disks"].get(name, {})
        if (current.get("major"), current.get("minor")) != (previous.get("major"), previous.get("minor")):
            continue
        prefix = f"disk/{name}/"
        changes = {field: rate(prefix + field + "_per_second", previous, current, field)
                   for field in ("reads", "writes", "read_bytes", "write_bytes", "read_ms", "write_ms")}
        rate(prefix + "busy_percent", previous, current, "busy_ms", .1)
        rate(prefix + "average_queue_depth", previous, current, "weighted_io_ms", .001)
        elapsed = [difference(previous, current, field) for field in ("read_ms", "write_ms")]
        operations = [changes["reads"], changes["writes"]]
        if all(value is not None for value in elapsed + operations) and sum(operations):
            values[prefix + "await_ms"] = sum(elapsed) / sum(operations)
        values[prefix + "inflight"] = current["inflight"]
    for name, value in new["memory_bytes"].items():
        values["memory/" + name + "_bytes"] = value
    for path, row in new["space"].items():
        for field in ("total_bytes", "free_bytes", "available_bytes", "available_inodes"):
            if field in row:
                values["space/" + path + "/" + field] = row[field]
    for group in GROUPS:
        for field in ("cpu_cores", "rss_bytes", "processes", "threads"):
            values[f"process/{group}/{field}"] = 0
    for pid, current in new["processes"].items():
        prefix = "process/" + current["group"] + "/"
        values[prefix + "rss_bytes"] += current["rss_bytes"]
        values[prefix + "processes"] += 1
        values[prefix + "threads"] += current["threads"]
        previous = old["processes"].get(pid)
        if (previous is None or previous["start_ticks"] != current["start_ticks"]
                or previous["group"] != current["group"]):
            # A newly started process's entire lifetime is in this interval.
            # Do not subtract counters belonging to a reused PID.
            previous = ({"user_ticks": 0, "system_ticks": 0}
                        if current["start_ticks"] >= old["uptime_seconds"] * ticks else {})
        usage = [difference(previous, current, field) for field in ("user_ticks", "system_ticks")]
        if all(value is not None for value in usage):
            values[prefix + "cpu_cores"] += sum(usage) / ticks / seconds
    return {"seconds": seconds, "started_at": before["at"], "finished_at": after["at"],
            "start_unix_seconds": before["unix_seconds"], "end_unix_seconds": after["unix_seconds"],
            "values": values, "counter_resets": resets}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def probe(url, timeout):
    started = time.monotonic()
    result = {"at": utc()}
    try:
        request = Request(url, headers={"User-Agent": "ucloud-build-telemetry/1"})
        with build_opener(NoRedirect).open(request, timeout=timeout) as response:
            result["status"] = response.status
            result["ok"] = 200 <= response.status < 300
    except HTTPError as exc:
        result.update(status=exc.code, ok=False)
        exc.close()
    except Exception as exc:
        result.update(ok=False, error=type(exc).__name__)
    result["latency_ms"] = (time.monotonic() - started) * 1000
    return result


class BoundedProbe:
    """Bound DNS and response-header stalls without accumulating probe threads."""
    def __init__(self):
        self.worker = None

    def run(self, url, timeout):
        if self.worker is not None and self.worker.is_alive():
            return {"at": utc(), "ok": False, "error": "PreviousProbeStillPending", "latency_ms": 0}
        result = []
        started = time.monotonic()
        self.worker = threading.Thread(target=lambda: result.append(probe(url, timeout)), daemon=True)
        self.worker.start()
        self.worker.join(timeout)
        if self.worker.is_alive():
            return {"at": utc(), "ok": False, "error": "ProbeDeadlineExceeded",
                    "latency_ms": (time.monotonic() - started) * 1000}
        return result[0]


def sample(args):
    if not 0 < args.duration <= 3600 or not .2 <= args.interval <= 60:
        raise ValueError("duration must be in (0,3600], interval in [0.2,60]")
    if not .05 <= args.health_timeout <= 5 or args.health_every < args.interval:
        raise ValueError("health timeout must be in [0.05,5], health frequency at least one sample interval")
    if args.health_url:
        parsed = urlsplit(args.health_url)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment):
            raise ValueError("health URL must be HTTP(S) without credentials, query or fragment")
    paths = sorted(set([path for path in DEFAULT_DISK_PATHS if Path(path).exists()] + args.disk_path))
    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    ticks = os.sysconf("SC_CLK_TCK")
    health_probe = BoundedProbe()
    metadata = {"type": "metadata", "schema": 1, "at": utc(), "label": args.label,
                "clock_ticks": ticks, "interval_seconds": args.interval, "duration_seconds": args.duration,
                "disk_paths": paths, "health_url": args.health_url, "limitations": LIMITATIONS}
    started = time.monotonic()
    deadline, due, probe_due, previous, count = started + args.duration, started, started, None, 0
    with args.output.open("x", encoding="utf-8") as output:
        output.write(json.dumps(metadata) + "\n")
        while not stopping:
            now = time.monotonic()
            if now < due:
                time.sleep(min(.1, due - now))
                continue
            row = {"type": "sample", "at": utc(), "unix_seconds": time.time(),
                   "monotonic_seconds": time.monotonic()}
            row["raw"] = snapshot(args.proc_root, paths)
            row["collection_ms"] = (time.monotonic() - now) * 1000
            if previous is not None:
                row["interval"] = derive(previous, row, ticks)
            remaining = deadline - time.monotonic()
            if args.health_url and now >= probe_due and remaining > .05:
                row["health"] = health_probe.run(args.health_url, min(args.health_timeout, remaining))
                probe_due = now + args.health_every
            output.write(json.dumps(row, separators=(",", ":"), allow_nan=False) + "\n")
            output.flush()
            previous, count = row, count + 1
            if now >= deadline:
                break
            due = min(deadline, max(due + args.interval, time.monotonic()))
        output.write(json.dumps({"type": "end", "at": utc(), "samples": count,
                                 "interrupted": stopping, "elapsed_seconds": time.monotonic() - started}) + "\n")


def boundary(value):
    if value is None:
        return None
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("window timestamps require an explicit timezone")
    return stamp.timestamp()


def summarize(args):
    begin, end = boundary(args.since), boundary(args.until)
    if begin is not None and end is not None and begin >= end:
        raise ValueError("since must precede until")
    metadata, previous, observations, health, intervals, skipped, collection = None, None, {}, [], [], 0, []
    with args.input.open(encoding="utf-8") as source:
        for line in source:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            if not isinstance(row, dict):
                skipped += 1
                continue
            if row.get("type") == "metadata":
                metadata = row
            if row.get("type") != "sample":
                continue
            if metadata is None:
                raise ValueError("metadata must precede samples")
            # Recompute from raw counters, so stored derived fields are auditable.
            interval = derive(previous, row, metadata["clock_ticks"]) if previous is not None else None
            previous = row
            if ((begin is None or row["unix_seconds"] >= begin)
                    and (end is None or row["unix_seconds"] <= end)):
                collection.append(row.get("collection_ms", 0))
                if "health" in row:
                    health.append(row["health"])
            if interval is None or (begin is not None and interval["start_unix_seconds"] < begin) or (
                    end is not None and interval["end_unix_seconds"] > end):
                continue
            intervals.append({key: interval[key] for key in ("seconds", "started_at", "finished_at", "counter_resets")})
            for key, value in interval["values"].items():
                observations.setdefault(key, []).append((interval["seconds"], value))
    summary = {}
    for key, points in observations.items():
        seconds = sum(weight for weight, _ in points)
        integral = sum(weight * value for weight, value in points)
        values = sorted(value for _, value in points)
        summary[key] = {"samples": len(points), "covered_seconds": seconds, "mean": integral / seconds,
                        "min": values[0], "max": values[-1], "p95": values[math.ceil(.95 * len(values)) - 1]}
        if key.endswith("_cores"):
            summary[key]["cpu_seconds"] = integral
        elif key.endswith("_per_second"):
            summary[key]["counter_delta"] = integral
    for key in list(summary):
        if key.startswith("disk/") and key.endswith("/await_ms"):
            prefix = key.rsplit("/", 1)[0] + "/"
            totals = [summary.get(prefix + name + "_per_second", {}).get("counter_delta")
                      for name in ("read_ms", "write_ms", "reads", "writes")]
            if all(value is not None for value in totals) and sum(totals[2:]):
                summary[key]["io_weighted_mean"] = sum(totals[:2]) / sum(totals[2:])
    latencies = sorted(row["latency_ms"] for row in health)
    report = {"metadata": metadata, "requested_since": args.since, "requested_until": args.until,
              "interval_count": len(intervals), "covered_seconds": sum(row["seconds"] for row in intervals),
              "started_at": intervals[0]["started_at"] if intervals else None,
              "finished_at": intervals[-1]["finished_at"] if intervals else None,
              "skipped_malformed_lines": skipped, "counter_resets": sum(len(row["counter_resets"]) for row in intervals),
              "max_collection_ms": max(collection, default=None), "summary": summary,
              "health": {"probes": len(health), "failures": sum(not row["ok"] for row in health),
                         "latency_p95_ms": latencies[math.ceil(.95 * len(latencies)) - 1] if latencies else None,
                         "max_latency_ms": max(latencies, default=None), "observations": health}}
    payload = json.dumps(report, indent=2, allow_nan=False) + "\n"
    if args.output:
        with args.output.open("x", encoding="utf-8") as output:
            output.write(payload)
    else:
        print(payload, end="")


def self_test():
    from copy import deepcopy
    from tempfile import TemporaryDirectory
    row = {"at": "2026-09-29T00:00:00+00:00", "unix_seconds": 100, "monotonic_seconds": 10,
           "raw": {"boot_id": "fixture", "uptime_seconds": 100, "online_cpus": 4,
                   "cpu_ticks": dict.fromkeys(CPU_FIELDS, 1000), "memory_bytes": {"MemAvailable": 9000},
                   "pressure": {"io": {"some_total": 1000}}, "vmstat": {}, "space": {},
                   "disks": {"sdb": dict(major=8, minor=16, reads=10, writes=10, read_bytes=1024,
                       write_bytes=2048, read_ms=50, write_ms=50, busy_ms=100, weighted_io_ms=200, inflight=0)},
                   "network": {"eth0": {"rx_bytes": 100, "rx_drops": 2}},
                   "processes": {"1": dict(group="buildkit", start_ticks=5000, user_ticks=10,
                                           system_ticks=10, rss_bytes=4096, threads=2)}}}
    after = deepcopy(row)
    after.update(monotonic_seconds=12, unix_seconds=102, at="2026-09-29T00:00:02+00:00")
    raw = after["raw"]
    raw["uptime_seconds"] = 102
    raw["cpu_ticks"].update(user=1100, system=1050, idle=1500, iowait=1100, softirq=1050)
    raw["pressure"]["io"]["some_total"] += 200000
    raw["disks"]["sdb"].update(writes=12, write_bytes=6144, write_ms=90, busy_ms=1100,
                               weighted_io_ms=4200, inflight=2)
    raw["network"]["eth0"].update(rx_bytes=2100, rx_drops=1)
    raw["processes"]["1"]["user_ticks"] += 100
    result = derive(row, after, 100)
    expected = {"cpu/busy_cores": 1, "cpu/iowait_cores": .5, "cpu/idle_cores": 2.5,
                "pressure/io/some_percent": 10, "disk/sdb/write_bytes_per_second": 2048,
                "disk/sdb/await_ms": 20, "disk/sdb/busy_percent": 50,
                "disk/sdb/average_queue_depth": 2, "network/eth0/rx_bytes_per_second": 1000,
                "process/buildkit/cpu_cores": .5, "process/buildkit/rss_bytes": 4096}
    for key, value in expected.items():
        assert result["values"][key] == value, (key, result["values"][key], value)
    assert "network/eth0/rx_drops_per_second" not in result["values"]
    assert result["counter_resets"] == ["network/eth0/rx_drops_per_second"]
    raw["processes"]["1"].update(start_ticks=10050, user_ticks=5, system_ticks=5)
    assert derive(row, after, 100)["values"]["process/buildkit/cpu_cores"] == .05
    with TemporaryDirectory() as directory:
        path, report = Path(directory) / "raw.jsonl", Path(directory) / "summary.json"
        samples = [dict(type="metadata", clock_ticks=100), dict(type="sample", **row),
                   dict(type="sample", **after)]
        later = deepcopy(after)
        later.update(monotonic_seconds=16, unix_seconds=106, at="2026-09-29T00:00:06+00:00")
        later["raw"]["cpu_ticks"]["user"] += 400
        samples.append(dict(type="sample", **later))
        path.write_text("\n".join(json.dumps(value) for value in samples) + '\n{"partial":')
        summarize(argparse.Namespace(input=path, output=report, since=None, until=None))
        summary = json.loads(report.read_text())
        assert summary["covered_seconds"] == 6 and summary["skipped_malformed_lines"] == 1
        assert summary["summary"]["cpu/busy_cores"]["cpu_seconds"] == 6
        assert summary["summary"]["disk/sdb/await_ms"]["io_weighted_mean"] == 20
        report.unlink()
        summarize(argparse.Namespace(input=path, output=report, since=utc(101), until=utc(106)))
        assert json.loads(report.read_text())["covered_seconds"] == 4
    raw["boot_id"] = "new-boot"
    assert derive(row, after, 100) is None
    fields = ["S", "1"] + ["0"] * 20
    fields[11], fields[12], fields[17], fields[19], fields[21] = "7", "8", "2", "99", "1"
    parsed = process_stat("12 (odd ) name) " + " ".join(fields))
    assert parsed["comm"] == "odd ) name" and parsed["user_ticks"] == 7 and parsed["start_ticks"] == 99
    for unit in ("ucloud-model-relay.service", "ucloud-sandbox-relay.service"):
        assert direct_group({"comm": "python3", "cgroup": "/system.slice/" + unit}) == "relay"
    assert direct_group({"comm": "python3", "cgroup":
                         "/system.slice/ucloud-build-load-client-cold.service"}) == "benchmark_driver"
    # A stalled DNS/header operation must not stall the sampler or accumulate
    # one new background request every health-probe interval.
    original, release = globals()["probe"], threading.Event()
    def stalled_probe(_url, _timeout):
        release.wait(1)
        return {"ok": True, "latency_ms": 0}
    checker = BoundedProbe()
    try:
        globals()["probe"] = stalled_probe
        started = time.monotonic()
        assert checker.run("http://unused.invalid/health", .05)["error"] == "ProbeDeadlineExceeded"
        assert time.monotonic() - started < .5
        assert checker.run("http://unused.invalid/health", .05)["error"] == "PreviousProbeStillPending"
    finally:
        release.set()
        if checker.worker is not None:
            checker.worker.join(1)
        globals()["probe"] = original
    print("self-test passed: CPU, I/O, PSI, network reset, PID reuse, reboot, process parsing, summary windows, bounded health")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    sampler = commands.add_parser("sample")
    sampler.add_argument("--output", type=Path, required=True)
    sampler.add_argument("--duration", type=float, default=1800)
    sampler.add_argument("--interval", type=float, default=2)
    sampler.add_argument("--label", default="host")
    sampler.add_argument("--disk-path", action="append", default=[])
    sampler.add_argument("--health-url")
    sampler.add_argument("--health-every", type=float, default=10)
    sampler.add_argument("--health-timeout", type=float, default=1)
    sampler.add_argument("--proc-root", type=Path, default=Path("/proc"))
    analyzer = commands.add_parser("summarize")
    analyzer.add_argument("--input", type=Path, required=True)
    analyzer.add_argument("--output", type=Path)
    analyzer.add_argument("--since")
    analyzer.add_argument("--until")
    commands.add_parser("self-test")
    args = parser.parse_args()
    try:
        {"sample": sample, "summarize": summarize, "self-test": lambda _: self_test()}[args.command](args)
    except (OSError, ValueError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
