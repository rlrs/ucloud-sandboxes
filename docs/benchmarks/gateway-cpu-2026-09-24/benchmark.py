import json, time, statistics
from pathlib import Path
from tempfile import TemporaryDirectory
from dataclasses import replace
from tests.test_control_plane import _sandbox_route, build_heartbeat
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.routing import RoutingStore
from ucloud_sandboxes.control_plane import (
    _route_only_sandbox_record,
    _sandbox_list_bytes,
)
from ucloud_sandboxes.fleet_reader import FleetResponseRenderer

with TemporaryDirectory() as tmp:
    c = ControlStateStore(Path(tmp) / "control")
    r = RoutingStore(Path(tmp) / "routes")
    for n in range(10):
        c.upsert_heartbeat(
            build_heartbeat(
                node_id=f"n{n}",
                job_id=f"j{n}",
                node_url=f"http://n{n}:8090",
                active_sandboxes=32,
            )
        )
    routes = []
    for n in range(320):
        route = _sandbox_route(
            sandbox_id=f"s{n:04}",
            node_id=f"n{n % 10}",
            job_id=f"j{n % 10}",
            node_url=f"http://n{n % 10}:8090",
            state="running",
            spec={
                "id": f"s{n:04}",
                "image": "example",
                "env": {f"ENV{i}": "x" * 25 for i in range(28)},
            },
        )
        r.upsert_sandbox(route)
        routes.append(route)

    def old():
        beats = {h.node_id: h for h in c.load_heartbeats().values()}
        records = [
            _route_only_sandbox_record(
                x, beats.get(x.node_id), heartbeat_ttl_seconds=120
            )
            for x in r.sandbox_routes_readonly(background=True)
        ]
        return json.dumps(
            {"sandboxes": records, "cached": True, "refresh_supported": True},
            separators=(",", ":"),
        ).encode()

    renderer = FleetResponseRenderer()

    def new():
        return _sandbox_list_bytes(c, r, 120, renderer=renderer)

    assert old() == new()
    for scenario in ("unchanged", "ten_percent_changed"):
        timings = {"old": [], "new": []}
        for i in range(30):
            if scenario != "unchanged":
                for n in range(32):
                    r.upsert_sandbox(
                        replace(routes[n], state="running" if i % 2 else "creating")
                    )
            for label, fn in (
                [("old", old), ("new", new)] if i % 2 else [("new", new), ("old", old)]
            ):
                t = time.process_time()
                fn()
                timings[label].append((time.process_time() - t) * 1000)
        med = {k: statistics.median(v) for k, v in timings.items()}
        print(
            json.dumps(
                {
                    "scenario": scenario,
                    "routes": 320,
                    "nodes": 10,
                    "iterations": 30,
                    "median_cpu_ms": med,
                    "reduction_percent": 100 * (1 - med["new"] / med["old"]),
                }
            )
        )
