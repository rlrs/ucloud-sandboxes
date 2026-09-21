"""Synthetic lifecycle metadata benchmark; does not contact production.

Run with PYTHONPATH=. .venv/bin/python scripts/benchmark_lifecycle_metadata.py
"""
from dataclasses import replace
import json
import os
from pathlib import Path
import platform
import sqlite3
import statistics
from tempfile import TemporaryDirectory
import time
from unittest.mock import patch

from ucloud_sandboxes.control_plane import _sandbox_transport_epoch
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.routing import RoutingStore, SandboxRoute


def milliseconds(operation, count=100):
    samples = []
    for _ in range(5):
        started = time.perf_counter()
        for _ in range(count):
            operation()
        samples.append((time.perf_counter() - started) * 1000 / count)
    return statistics.median(samples)


def main():
    with TemporaryDirectory() as directory:
        root = Path(directory)
        store = RoutingStore(root / "routes.sqlite")
        route = store.upsert_sandbox(SandboxRoute(
            sandbox_id="target", node_id="node", job_id="job", node_url="http://node",
            resources=ResourceQuantity(), spec={"id": "target"}, generation=1,
            create_operation_id="create", spec_hash="a" * 64, state="parked",
        ))
        migration = store.begin_sandbox_migration(
            route, migration_id="move-target", destination_node_id="dest",
            destination_job_id="dest-job", destination_node_url="http://dest",
        )
        # Match the retained production history count. Empty snapshot metadata
        # deliberately avoids exaggerating JSON decode costs in this fixture.
        with store._transaction() as connection:
            for index in range(268):
                store._write_sandbox_migration(connection, replace(
                    migration, migration_id=f"move-{index}", phase="complete",
                    sandbox_id="target" if index == 0 else f"other-{index}",
                ))
            connection.execute("DELETE FROM sandbox_migrations WHERE migration_id = 'move-target'")
        def epoch(scoped):
            return _sandbox_transport_epoch(route, store.sandbox_migrations(
                active_only=False, **({"sandbox_id": route.sandbox_id} if scoped else {}),
            ))
        assert epoch(False) == epoch(True)
        result = {
            "platform": platform.platform(), "python": platform.python_version(),
            "history_rows": 268, "relevant_rows": 1,
            "epoch_all_history_ms": milliseconds(lambda: epoch(False)),
            "epoch_scoped_history_ms": milliseconds(lambda: epoch(True)),
            "permission_chmod_calls_per_100_reads": {},
        }
        for cls in (RoutingStore, ControlStateStore):
            path = root / f"{cls.__name__}.sqlite"
            db = cls(path)
            def read():
                if isinstance(db, RoutingStore):
                    return db.get_sandbox_readonly("absent")
                return db.get_heartbeat("absent")
            keeper = sqlite3.connect(path)
            try:
                keeper.execute("SELECT name FROM sqlite_schema").fetchall()
                read()
                with patch("os.chmod", wraps=os.chmod) as chmod:
                    for _ in range(100):
                        read()
                    after = chmod.call_count
                # Previous implementation unconditionally chmod'ed every file
                # each time its permission helper ran. Reproduce just that helper.
                def previous_permissions(*_args):
                    for suffix in ("", "-wal", "-shm"):
                        try:
                            os.chmod(Path(f"{path}{suffix}"), 0o600)
                        except FileNotFoundError:
                            pass
                target = ("ucloud_sandboxes.routing._chmod_sqlite_state_files"
                          if isinstance(db, RoutingStore)
                          else "ucloud_sandboxes.control_state.ControlStateStore._secure_files")
                with patch(target, previous_permissions), patch("os.chmod", wraps=os.chmod) as chmod:
                    for _ in range(100):
                        read()
                    before = chmod.call_count
                result["permission_chmod_calls_per_100_reads"][cls.__name__] = {
                    "before": before, "after": after,
                }
            finally:
                keeper.close()
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
