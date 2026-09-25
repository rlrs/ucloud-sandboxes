"""Exercise real gateway/worker HTTP contracts with PostgreSQL routing."""

import json
import unittest
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tests.test_control_plane import ControlPlaneTests
from tests.test_postgres_routing import DSN, PostgresRoutingContracts


@unittest.skipUnless(DSN, "requires isolated PostgreSQL")
class PostgresGatewayContracts(ControlPlaneTests):
    def setUp(self):
        from ucloud_sandboxes.shared_control.routing_repository import (
            PostgresRoutingStore,
        )

        PostgresRoutingContracts.setUp(self)
        self.factory = lambda path: self.store(path)
        # The original HTTP fault-injection test patches this domain method on
        # its constructor. Forward that patch to the actual PostgreSQL instance.
        self.factory.allocate_sandbox_create_with_pending = (
            PostgresRoutingStore.allocate_sandbox_create_with_pending
        )
        self.gateway_patches = [
            patch("tests.test_control_plane.RoutingStore", self.factory),
            patch("ucloud_sandboxes.control_plane.open_routing_store", self.factory),
        ]
        for item in self.gateway_patches:
            item.start()

    def store(self, path):
        store = PostgresRoutingContracts.store(self, path)
        store.allocate_sandbox_create_with_pending = lambda *args, **kwargs: (
            self.factory.allocate_sandbox_create_with_pending(store, *args, **kwargs)
        )
        if not path.exists() and path.parent.exists():
            dsn = path.with_suffix(".dsn")
            dsn.write_text(DSN)
            path.write_text(
                json.dumps(
                    {
                        "format": "ucloud-postgres-routing-v1",
                        "schema": store.schema,
                        "dsn_file": str(dsn.resolve()),
                    }
                )
            )
        return store

    _store = PostgresRoutingContracts._store

    # That SQLite test expects a failure from the removed process-lock path.
    test_gateway_placement_contention_deadline_returns_retryable_json = None

    def test_postgres_placement_does_not_wait_for_legacy_process_lock(self):
        from tempfile import TemporaryDirectory
        from ucloud_sandboxes import control_plane
        from ucloud_sandboxes.models import ResourceQuantity

        with TemporaryDirectory() as directory:
            handler = object.__new__(control_plane.ControlPlaneHandler)
            handler.routing_store = self.store(Path(directory) / "routes.sqlite")
            handler._select_node = lambda *a, **kw: None
            with (
                control_plane._GATEWAY_SCHEDULING_LOCK,
                ThreadPoolExecutor(1) as executor,
            ):
                result = executor.submit(
                    handler._select_and_reserve_node,
                    "s",
                    ResourceQuantity(),
                    spec={"id": "s"},
                    spec_hash="a" * 64,
                ).result(timeout=0.5)
                self.assertIsNone(result)

    def tearDown(self):
        for item in self.gateway_patches:
            item.stop()
        PostgresRoutingContracts.tearDown(self)
