"""Read-only placement scan reproduction with competing Python CPU work.

The baseline is the prior per-row reader. No production access is needed.
"""
from __future__ import annotations
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread
from time import monotonic
from statistics import median
from tests.test_control_plane import _sandbox_route, build_heartbeat
from ucloud_sandboxes.control_state import ControlStateStore, _placement_heartbeat
from ucloud_sandboxes.routing import RoutingStore, SandboxRoute, _sandbox_route_from_row

def baseline(
    self,
    *,
    node_id: str,
    job_id: str,
    node_url: str,
) -> list[SandboxRoute]:
    cleaned_node_url = node_url.strip().rstrip("/")
    node_url_with_slash = f"{cleaned_node_url}/" if cleaned_node_url else ""
    with self._connect() as conn:
        return [
            route
            for route in (
                _sandbox_route_from_row(row)
                for row in conn.execute(
                    """
                    SELECT sandbox_id, node_id, job_id, node_url,
                           resources_json, spec_json, state, generation,
                           create_operation_id, spec_hash, delete_operation_id,
                           node_epoch, activity_epoch, worker_state,
                           storage_schema,
                           snapshot_manifest_digest, snapshot_repository,
                           snapshot_tag, storage_snapshot_json,
                           created_at, updated_at
                    FROM sandboxes
                    WHERE node_id = ? OR job_id = ?
                       OR node_url IN (?, ?)
                    ORDER BY sandbox_id
                    """,
                    (
                        node_id.strip(),
                        job_id.strip(),
                        cleaned_node_url,
                        node_url_with_slash,
                    ),
                )
            )
            if route is not None
        ]


def cpu_load(stop):
    while not stop.is_set():
        sum(i * i for i in range(1000))


if __name__ == '__main__':
    with TemporaryDirectory() as directory:
        store = RoutingStore(Path(directory) / 'routes.sqlite')
        for i in range(320):
            store.upsert_sandbox(_sandbox_route(
                sandbox_id=f's{i:04}', node_id=f'n{i % 10}', job_id=f'j{i % 10}',
                node_url=f'http://n{i % 10}',
                spec={'id': f's{i:04}', 'image': 'test', 'env': {'DATA': 'x' * 1024}},
            ))
        control = ControlStateStore(Path(directory) / 'control.sqlite')
        for i in range(10):
            control.upsert_heartbeat(build_heartbeat(
                node_id=f'n{i}', job_id=f'j{i}', node_url=f'http://n{i}',
            ))
        def baseline_heartbeats():
            with control._transaction(write=False) as connection:
                return {k: _placement_heartbeat(v)
                        for k, v in control._load_heartbeats(connection).items()}
        assert baseline_heartbeats() == control.load_heartbeats()
        kwargs = {'node_id': 'n0', 'job_id': 'j0', 'node_url': 'http://n0'}
        assert baseline(store, **kwargs) == store.sandbox_routes_matching_node_identity(**kwargs)
        for competitors in (0, 1, 2):
            stop = Event()
            threads = [Thread(target=cpu_load, args=(stop,)) for _ in range(competitors)]
            for thread in threads:
                thread.start()
            samples = {'before': [], 'after': [], 'heartbeat_before': [], 'heartbeat_after': []}
            try:
                for _ in range(12):
                    for name, read in (
                        ('before', lambda: baseline(store, **kwargs)),
                        ('after', lambda: store.sandbox_routes_matching_node_identity(**kwargs)),
                        ('heartbeat_before', baseline_heartbeats),
                        ('heartbeat_after', control.load_heartbeats),
                    ):
                        started = monotonic()
                        assert len(read()) == (10 if name.startswith('heartbeat') else 32)
                        samples[name].append((monotonic() - started) * 1000)
            finally:
                stop.set()
                for thread in threads:
                    thread.join()
            results = {k: round(median(v), 3) for k, v in samples.items()}
            print(json.dumps({'cpu_competitors': competitors, 'routes': 320,
                              'selected': 32, 'median_ms': results}), flush=True)
