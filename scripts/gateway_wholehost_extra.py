#!/usr/bin/env python3
"""Bounded supplemental sar, memory, per-CPU softirq and qdisc capture.

No process arguments, environment, credentials, or workload bodies are read.
The caller can run this as a nice-10 transient systemd service.
"""

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import time


def read(path):
    try:
        return Path(path).read_text()
    except OSError:
        return ""


def numeric_pairs(path):
    values = {}
    for line in read(path).splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[1].isdigit():
            name = fields[0].rstrip(":")
            byte_unit = len(fields) > 2 and fields[2] == "kB"
            values[name + ("_bytes" if byte_unit else "")] = int(fields[1]) * (1024 if byte_unit else 1)
    return values


def softirqs():
    lines = read("/proc/softirqs").splitlines()
    cpus = lines[0].split() if lines else []
    result = {}
    for line in lines[1:]:
        fields = line.split()
        if fields:
            result[fields[0].rstrip(":")] = {cpu: int(value) for cpu, value in zip(cpus, fields[1:]) if value.isdigit()}
    return result


def qdiscs():
    try:
        captured = subprocess.run(["tc", "-j", "-s", "qdisc", "show"], check=True,
                                  capture_output=True, text=True, timeout=3)
        keep = {"kind", "handle", "dev", "parent", "root", "bytes", "packets", "drops",
                "overlimits", "requeues", "backlog", "qlen"}
        return [{key: value for key, value in row.items() if key in keep}
                for row in json.loads(captured.stdout)]
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--unit", required=True)
    parser.add_argument("--duration", type=float, default=1200)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--block-device", help="Optional registry block device name, such as sdb")
    args = parser.parse_args()
    if not 0 < args.duration <= 1200 or not math.isfinite(args.interval) or not 1 <= args.interval <= 60:
        parser.error("duration must be 0–1200 seconds; interval must be 1–60 seconds")
    if args.block_device and not re.fullmatch(r"[A-Za-z0-9_.-]+", args.block_device):
        parser.error("block-device must be a device name, not a path")
    args.directory.mkdir(parents=True, exist_ok=False)
    metadata = {"unit": args.unit, "sampler_pid": os.getpid(), "started_at": datetime.now(timezone.utc).isoformat(),
                "duration_limit_seconds": args.duration, "snapshot_interval_seconds": args.interval,
                "sar_interval_seconds": 2, "kernel": os.uname().release,
                "online_cpus_at_start": read("/sys/devices/system/cpu/online").strip()}
    metadata["block_device"] = args.block_device
    (args.directory / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    stopped = False

    def stop(_signum, _frame):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    started = time.monotonic()
    with (args.directory / "sar.stderr").open("w") as errors:
        sar = subprocess.Popen(["sar", "-A", "-P", "ALL", "-I", "ALL", "-o",
                                str(args.directory / "sar.bin"), "2", str(math.ceil(args.duration / 2))],
                               stdout=subprocess.DEVNULL, stderr=errors)
        metadata["sar_pid"] = sar.pid
        (args.directory / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        try:
            with (args.directory / "supplemental.jsonl").open("w") as output:
                while not stopped and time.monotonic() - started < args.duration:
                    before = time.monotonic()
                    row = {"at": datetime.now(timezone.utc).isoformat(), "unix_seconds": time.time(),
                           "online_cpus": read("/sys/devices/system/cpu/online").strip(),
                           "meminfo": numeric_pairs("/proc/meminfo"), "vmstat": numeric_pairs("/proc/vmstat"),
                           "softirqs": softirqs(), "qdiscs": qdiscs(), "sar_running": sar.poll() is None}
                    if args.block_device:
                        raw = read("/sys/class/block/" + args.block_device + "/stat").split()
                        row["registry_block_stat"] = [int(value) for value in raw if value.isdigit()]
                    row["collection_seconds"] = time.monotonic() - before
                    output.write(json.dumps(row, separators=(",", ":")) + "\n")
                    output.flush()
                    while not stopped and time.monotonic() < before + args.interval:
                        time.sleep(min(.2, max(0, before + args.interval - time.monotonic())))
        finally:
            if sar.poll() is None:
                sar.terminate()
            try:
                sar.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sar.kill()
                sar.wait()
            metadata.update(finished_at=datetime.now(timezone.utc).isoformat(), sar_exit_code=sar.returncode,
                            elapsed_seconds=time.monotonic() - started,
                            online_cpus_at_finish=read("/sys/devices/system/cpu/online").strip())
            (args.directory / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
