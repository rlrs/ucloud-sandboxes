"""CPU comparison against the previous placement scorer, without database I/O."""
from __future__ import annotations
import json
import statistics
import time
from tests.test_control_plane import build_heartbeat, _sandbox_route
from ucloud_sandboxes.control_plane import (
    NodePlacementState, PlacementRecord, SandboxRoute, _heartbeat_has_image,
    _node_available_resources, _node_placement_state, canonical_image_digest_ref,
)
from ucloud_sandboxes.models import NodeHeartbeat, ResourceQuantity

def _route_image_identity(route: PlacementRecord) -> str:
    image = (
        str(route.spec.get("image") or "")
        if isinstance(route, SandboxRoute)
        else route.image
    ).strip()
    return canonical_image_digest_ref(image) or image


def baseline(
    heartbeat: NodeHeartbeat,
    node_routes: list[PlacementRecord],
) -> NodePlacementState:
    inflight_images = frozenset(
        identity
        for route in node_routes
        if route.state.lower() in {"creating", "unknown"}
        and (identity := _route_image_identity(route))
        and not _heartbeat_has_image(heartbeat, identity)
    )
    projected_images = set(heartbeat.cached_images)
    projected_images.update(
        identity
        for route in node_routes
        if route.state.lower() in {"creating", "unknown", "running"}
        and (identity := _route_image_identity(route))
    )
    # Completed creates still own future work, including parked programs.
    # Their shape is a relative load estimate, not a capacity reservation or
    # an admission limit. Live pressure alone lags a burst by a heartbeat and
    # otherwise rewards repeatedly placing onto the same quiet worker.
    assigned = [r for r in node_routes if r.state.lower() not in {"deleted", "failed"}]
    total = heartbeat.total_resources
    assigned_pressure = max(
        sum(r.resources.vcpu for r in assigned) / max(1, total.vcpu),
        sum(r.resources.memory_mb for r in assigned) / max(1, total.memory_mb),
    )
    return NodePlacementState(
        assigned_shape_pressure=assigned_pressure,
        available_resources=_node_available_resources(heartbeat, node_routes),
        inflight_image_identities=inflight_images,
        projected_image_identities=frozenset(projected_images),
        active_creates=max(
            heartbeat.active_sandbox_creates,
            sum(
                route.state.lower()
                in {
                    "creating",
                    "planned",
                    "quota_ready",
                    "rootfs_ready",
                    "unknown",
                }
                for route in node_routes
            ),
        ),
    )

if __name__ == '__main__':
    for images in (1, 8, 32):
        heartbeat = build_heartbeat(
            node_id='n', job_id='j', node_url='http://n',
            total_resources=ResourceQuantity(32, 98304, 1000000),
            cached_images=tuple(f'registry/image{i}@sha256:{i:064x}' for i in range(256)),
        )
        routes = [_sandbox_route(
            sandbox_id=f's{i}', node_id='n', job_id='j', node_url='http://n',
            state='creating' if i % 2 else 'running',
            spec={'id': f's{i}', 'image': f'registry/image{i % images}@sha256:{i % images:064x}'},
            resources=ResourceQuantity(1, 2048, 4096),
        ) for i in range(32)]
        assert baseline(heartbeat, routes) == _node_placement_state(heartbeat, routes)
        samples = {baseline: [], _node_placement_state: []}
        for _ in range(7):
            for fn in (baseline, _node_placement_state):
                start = time.process_time()
                for _ in range(2000):
                    fn(heartbeat, routes)
                samples[fn].append((time.process_time() - start) / 2000 * 1e6)
        before, after = (statistics.median(samples[f]) for f in (baseline, _node_placement_state))
        print(json.dumps({'routes_per_node': 32, 'distinct_images': images,
                          'before_cpu_us': before, 'after_cpu_us': after,
                          'reduction_percent': 100 * (1 - after / before)}))
