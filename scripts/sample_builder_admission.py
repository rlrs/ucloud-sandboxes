#!/usr/bin/env python3
"""Read-only, indexed admission counters for an owned builder qualification."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import signal
import sqlite3
import subprocess
import time


PHASE = "json_extract(record_json, '$.admission_phase')"
STATUS = "json_extract(record_json, '$.status')"
PHASE_TYPE = "json_type(record_json, '$.admission_phase')"
OWNED = (f"COALESCE({STATUS}, '') NOT IN ('succeeded', 'failed') "
         f"OR COALESCE({PHASE}, '') NOT IN ('', 'released') "
         f"OR COALESCE({PHASE_TYPE}, 'text') != 'text'")
QUERY = (f"SELECT {STATUS}, {PHASE}, {PHASE_TYPE}, COUNT(*) "
         f"FROM image_state_v1_builds WHERE {OWNED} GROUP BY {STATUS}, {PHASE}, {PHASE_TYPE}")


def stamp():
    return datetime.now(timezone.utc).isoformat()


def require(value, message):
    if not value:
        raise ValueError(message)


def discover_database(expected_job_id):
    result = subprocess.run(["systemctl", "show", "ucloud-sandbox-node.service",
                             "--property=MainPID", "--value"],
                            capture_output=True, text=True, timeout=5, check=True)
    pid = int(result.stdout)
    require(pid > 1, "Builder service is not running")
    argv = Path(f"/proc/{pid}/cmdline").read_bytes().decode().rstrip("\0").split("\0")
    require(argv[1:4] == ["-m", "ucloud_sandboxes.cli", "serve-builder-agent"], "Unexpected service")
    for flag in ("--job-id", "--image-file"):
        require(argv.count(flag) == 1 and argv.index(flag) + 1 < len(argv), "Service identity absent")
    require(argv[argv.index("--job-id") + 1] == expected_job_id, "Unexpected owned node")
    path = Path(argv[argv.index("--image-file") + 1])
    require(path.is_absolute() and path.is_file(), "Image database absent")
    return path


def connect(path):
    # URI mode=ro cannot create a missing state file; query_only also fences
    # accidental writes if this diagnostic grows later.
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True,
                         timeout=0.1, isolation_level=None)
    try:
        db.execute("PRAGMA query_only=ON")
        plan = list(db.execute("EXPLAIN QUERY PLAN " + QUERY))
        require(any("image_build_owned_phases" in row[3] for row in plan),
                "Candidate admission index absent; refusing retained-history scan")
    except BaseException:
        db.close()
        raise
    return db


def sample(db):
    started = time.monotonic()
    db.set_progress_handler(lambda: int(time.monotonic() - started > 0.1), 1000)
    active = preparing = finishing = cleanup = 0
    try:
        for status, phase, kind, count in db.execute(QUERY):
            if kind is None:
                phase = ""
            require(status in {"running", "succeeded", "failed"}
                    and kind in {None, "text"}
                    and phase in {"", "preparing_solving", "finishing", "released"},
                    "Invalid admission metadata")
            active += count
            finishing += count if phase == "finishing" else 0
            preparing += count if phase != "finishing" else 0
            cleanup += count if status in {"succeeded", "failed"} else 0
    finally:
        db.set_progress_handler(None, 0)
    counts = {"active_builds": active, "preparing_solving_builds": preparing,
              "finishing_builds": finishing, "terminal_cleanup_builds": cleanup}
    return {**counts, "probe_ms": (time.monotonic() - started) * 1000,
            "violations": [key for key, limit in (("active_builds", 6),
                            ("preparing_solving_builds", 4), ("finishing_builds", 2))
                           if counts[key] > limit]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-job-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=1800)
    parser.add_argument("--interval", type=float, default=2)
    args = parser.parse_args()
    require(0 < args.duration <= 3600 and 1 <= args.interval <= 30, "Unbounded sample settings")
    database = discover_database(args.expected_job_id)
    db = connect(database)
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    started = time.monotonic()
    samples = errors = violations = 0
    with args.output.open("x") as output:
        args.output.chmod(0o600)
        output.write(json.dumps({"type": "metadata", "at": stamp(), "job_id": args.expected_job_id,
                                 "interval_seconds": args.interval, "duration_seconds": args.duration,
                                 "read_only": True, "indexed": True, "bounds": [6, 4, 2]}) + "\n")
        try:
            while not stopping and time.monotonic() - started < args.duration:
                due = time.monotonic() + args.interval
                try:
                    counts = sample(db)
                    samples += 1
                    violations += bool(counts["violations"])
                    row = {"type": "sample", "at": stamp(), **counts}
                except Exception as error:
                    errors += 1
                    row = {"type": "error", "at": stamp(), "error_type": type(error).__name__}
                output.write(json.dumps(row, separators=(",", ":")) + "\n")
                output.flush()
                time.sleep(max(0, min(due - time.monotonic(), args.duration - (time.monotonic() - started))))
        finally:
            db.close()
            output.write(json.dumps({"type": "end", "at": stamp(), "samples": samples,
                                     "errors": errors, "violating_samples": violations,
                                     "interrupted": stopping}) + "\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps({"complete": False, "error_type": type(error).__name__}))
        raise SystemExit(1) from None
