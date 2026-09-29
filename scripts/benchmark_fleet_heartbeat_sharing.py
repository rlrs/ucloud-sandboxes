"""Compare detached/shared heartbeat reads during complete fleet rendering.

Uses only synthetic temporary SQLite databases. Both variants preserve full
inventories and use identical route reads and persistent response renderers.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
import json
from pathlib import Path
import platform
import statistics
from tempfile import TemporaryDirectory
import time

from ucloud_sandboxes.agent import build_heartbeat
from ucloud_sandboxes.control_plane import _sandbox_list_bytes
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.fleet_reader import FleetResponseRenderer
from ucloud_sandboxes.models import ResourceQuantity, SandboxInventoryEntry
from ucloud_sandboxes.routing import RoutingStore, SandboxRoute


class ReadMode:
    def __init__(self, store, shared):
        self.store, self.shared = store, shared

    def load_heartbeats(self, **_kwargs):
        return self.store.load_heartbeats(shared=self.shared)


def benchmark(root, *, sandboxes, reads, repeats):
    control = ControlStateStore(root / "control.sqlite")
    routes = RoutingStore(root / "routes.sqlite")
    inventories = [[], [], []]
    resources = ResourceQuantity(1, 1024, 4096)
    for index in range(sandboxes):
        node = index % 3
        sandbox_id = f"sandbox-{index:04}"
        layers = [{"digest": "sha256:" + str(layer) * 64, "size": 32 * 1024**2}
                  for layer in range(4)]
        inventories[node].append(SandboxInventoryEntry(
            sandbox_id=sandbox_id, generation=1, operation_id="create",
            spec_hash="a" * 64, state="running", resources=resources,
            storage_schema="storage-native-v1",
            snapshot_manifest_digest="sha256:" + "b" * 64,
            snapshot_repository="bench/snapshots", snapshot_tag=sandbox_id,
            storage_snapshot={"version": 1, "layers": layers},
            storage_dependency={"layers": layers},
        ))
        routes.upsert_sandbox(SandboxRoute(
            sandbox_id=sandbox_id, node_id=f"node-{node}", job_id=f"job-{node}",
            node_url=f"http://worker-{node}:8090", resources=resources,
            state="running", generation=1, create_operation_id="create",
            spec_hash="a" * 64,
            spec={"id": sandbox_id, "image": "registry.test/python@sha256:" + "c" * 64,
                  "cpus": 1, "memory_mb": 1024, "disk_mb": 4096,
                  "parkable": True, "managed_process": True,
                  "labels": {"owner": "benchmark", "purpose": "fleet-read"}},
        ))
    for node, inventory in enumerate(inventories):
        heartbeat = build_heartbeat(
            job_id=f"job-{node}", node_id=f"node-{node}", deployment_id="benchmark",
            node_url=f"http://worker-{node}:8090",
        )
        control.upsert_heartbeat(replace(
            heartbeat, active_sandboxes=len(inventory), inventory=tuple(inventory),
            inventory_complete=True, labels={"pool": "workers", "zone": "test"},
        ))
    modes = {name: (ReadMode(control, shared), FleetResponseRenderer())
             for name, shared in (("detached", False), ("shared", True))}
    expected = None
    for reader, renderer in modes.values():
        result = _sandbox_list_bytes(reader, routes, 3600, renderer=renderer)
        if expected is None:
            expected = result
        assert result == expected, "read mode changed the complete response"
    samples = {name: [] for name in modes}
    for repeat in range(repeats):
        for name in list(modes)[::(-1 if repeat % 2 else 1)]:
            reader, renderer = modes[name]
            cpu, wall = time.process_time(), time.perf_counter()
            for _ in range(reads):
                result = _sandbox_list_bytes(reader, routes, 3600, renderer=renderer)
                assert result == expected, "response changed during benchmark"
            samples[name].append({
                "cpu_ms_per_read": (time.process_time() - cpu) * 1000 / reads,
                "wall_ms_per_read": (time.perf_counter() - wall) * 1000 / reads,
            })
    medians = {name: statistics.median(s["cpu_ms_per_read"] for s in values)
               for name, values in samples.items()}
    with control._connection() as connection:
        payload_bytes = connection.execute(
            "SELECT sum(length(payload)) FROM control_records WHERE namespace='heartbeat'",
        ).fetchone()[0]
    return {"nodes": 3, "inventory_entries": sandboxes, "routes": sandboxes,
            "heartbeat_payload_bytes": payload_bytes, "response_bytes": len(expected),
            "response_bytes_identical": True, "median_cpu_ms_per_read": medians,
            "cpu_reduction_percent": 100 * (1 - medians["shared"] / medians["detached"]),
            "samples": samples}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sandboxes", type=int, default=500)
    parser.add_argument("--reads", type=int, default=100)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.sandboxes, args.reads, args.repeat) < 1:
        parser.error("sandboxes, reads and repeat must be positive")
    with TemporaryDirectory() as directory:
        result = benchmark(Path(directory), sandboxes=args.sandboxes,
                           reads=args.reads, repeats=args.repeat)
    report = {"python": platform.python_version(), "platform": platform.platform(),
              "reads_per_sample": args.reads, "repeats": args.repeat,
              "scope": "unchanged full fleet reads, including SQLite route/heartbeat reads; not production or end-to-end qualification",
              **result}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
