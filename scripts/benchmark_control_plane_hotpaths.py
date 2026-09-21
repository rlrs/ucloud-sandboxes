"""Local, synthetic control-plane benchmark. Never contacts production."""
from __future__ import annotations

import argparse
from dataclasses import replace
import json
from pathlib import Path
import platform
import sqlite3
from tempfile import TemporaryDirectory
import time

from ucloud_sandboxes.agent import build_heartbeat
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.metrics import MetricsStore
from ucloud_sandboxes.models import ResourceQuantity, SandboxInventoryEntry


def benchmark_reads(root: Path) -> dict:
    store = ControlStateStore(root / "control.sqlite")
    for node in range(6):
        heartbeat = build_heartbeat(
            job_id=f"job-{node}", node_id=f"node-{node}", deployment_id="benchmark",
            node_url=f"http://node-{node}:8090",
        )
        inventory = tuple(SandboxInventoryEntry(
            sandbox_id=f"sandbox-{node}-{index}", generation=1,
            operation_id="create-1", spec_hash="a" * 64, state="running",
            resources=ResourceQuantity(1, 1024, 5184),
            storage_dependency={"layers": [
                {"digest": "sha256:" + "b" * 64, "size": 1024} for _ in range(4)
            ]},
        ) for index in range(86))
        store.upsert_heartbeat(replace(heartbeat, inventory=inventory, inventory_complete=True))
    results = {}
    for name, load, count in (
        ("owner_reads", lambda: store.get_heartbeat("job-0"), 1000),
        ("fleet_reads", store.load_heartbeats, 200),
    ):
        load()
        cpu, wall = time.process_time(), time.perf_counter()
        for _ in range(count):
            load()
        results[name] = {
            "count": count, "cpu_seconds": time.process_time() - cpu,
            "wall_seconds": time.perf_counter() - wall,
        }
    return results


def benchmark_metrics(root: Path) -> dict:
    path = root / "metrics.sqlite"
    store = MetricsStore(path, max_bytes=128 * 1024, max_events=10)
    store.append("initial")
    reader = sqlite3.connect(path)
    try:
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM metric_events").fetchall()
        started = time.perf_counter()
        for index in range(8):
            store.append("event", {"index": index, "padding": "x" * 2048})
        elapsed = time.perf_counter() - started
    finally:
        reader.close()
    store.append("after-reader")
    return {"append_count": 8, "wall_seconds": elapsed,
            "retained_events": len(store.load_events())}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeat", type=int, default=3)
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error("repeat must be positive")
    runs = []
    for _ in range(args.repeat):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            runs.append({"reads": benchmark_reads(root), "metrics": benchmark_metrics(root)})
    print(json.dumps({"python": platform.python_version(), "platform": platform.system(),
                      "nodes": 6, "inventory_per_node": 86, "runs": runs}, indent=2))


if __name__ == "__main__":
    main()
