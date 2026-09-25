"""Run the existing lifecycle/domain contracts against real PostgreSQL."""

from pathlib import Path
import os
import unittest
from unittest.mock import patch
from uuid import uuid4
from threading import Lock

from tests.test_routing import RoutingStoreTests

DSN = os.environ.get("UCLOUD_TEST_POSTGRES_DSN")


@unittest.skipUnless(DSN, "requires isolated PostgreSQL")
class PostgresRoutingContracts(unittest.TestCase):
    def setUp(self):
        self.schemas = {}
        self.stores = []
        self.guard = Lock()
        self.patcher = patch("tests.test_routing.RoutingStore", self.store)
        self.patcher.start()

    def store(self, path):
        with self.guard:
            return self._store(path)

    def _store(self, path):
        from ucloud_sandboxes.shared_control.routing_repository import (
            PostgresRoutingStore,
        )

        path = Path(path)
        fresh = path not in self.schemas
        schema = self.schemas.setdefault(path, "ucloud_routing_test_" + uuid4().hex)
        store = PostgresRoutingStore(path, dsn=DSN, schema=schema)
        self.stores.append(store)
        if fresh:
            store.migrate()
        return store

    def tearDown(self):
        import psycopg
        from psycopg import sql

        self.patcher.stop()
        for store in self.stores:
            store.close()
        with psycopg.connect(DSN, autocommit=True) as conn:
            for schema in self.schemas.values():
                conn.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )


# SQLite file-format, process-writer and filesystem-permission tests remain in
# the standalone suite. The domain rules below are identical on both backends.
CONTRACTS = (
    "observation_cannot_recreate_deleted_or_deleting_incarnation",
    "active_migration_lookup_is_scoped_to_sandbox",
    "lifecycle_proof_fences_pre_mutation_heartbeats",
    "exec_route_requires_exact_worker_identity",
    "exec_route_replay_cannot_change_worker_identity",
    "generic_route_upsert_accepts_internal_transition_state",
    "generic_route_upsert_cannot_reassign_worker_owner",
    "managed_primary_state_survives_route_handoff_and_is_generation_fenced",
    "program_request_lifecycle_is_monotonic_durable_and_terminal",
    "program_request_identity_is_generation_fenced",
    "delete_intent_terminalizes_program_requests_transactionally",
    "migration_pending_shape_excludes_source_job",
    "concurrent_writes_preserve_valid_state",
    "migration_journal_and_route_switch_commit_atomically",
    "route_delete_terminalizes_active_migration",
    "orphaned_migration_reconciliation_is_bounded",
    "wake_completion_atomically_marks_destination_waking",
    "reconcile_sandboxes_for_node_removes_missing_node_routes",
    "reconcile_sandboxes_for_node_keeps_newer_routes",
    "reconcile_inventory_cannot_advance_route_generation",
    "reconcile_inventory_promotes_completed_background_publication",
    "reconcile_inventory_rejects_snapshot_for_another_incarnation",
    "transient_inventory_cannot_regress_stable_lifecycle_state",
    "non_routable_inventory_only_proves_sandbox_presence",
    "delete_sandboxes_for_jobs_removes_routes_and_dependents",
    "node_loss_keeps_only_fully_published_parked_route",
    "worker_detach_is_generation_fenced_and_idempotent",
    "delete_stale_sandboxes_removes_missing_jobs_after_grace",
    "route_claims_image_specific_capacity_before_generic_capacity",
    "image_warmup_survives_automatic_prepared_capacity_claim",
    "transient_capacity_signals_are_consumable",
    "expired_signals_are_pruned_by_their_public_read_paths",
    "allocation_returns_and_consumes_pending_demand_atomically",
    "failed_create_pending_demand_preserves_incarnation_identity",
    "post_placement_failures_do_not_request_fleet_capacity",
    "snapshot_consume_does_not_delete_refreshed_signals",
    "stale_inventory_cannot_overwrite_or_delete_newer_generation",
    "same_generation_update_requires_exact_nonempty_identity",
    "route_incarnation_normalizes_node_url_for_every_mutation",
    "exact_identity_adopts_new_boot_epoch_then_allows_absence",
    "refresh_fence_cannot_readopt_or_delete_from_retired_boot",
    "new_boot_inventory_removes_absent_old_boot_process",
    "new_boot_inventory_detaches_absent_portable_park",
    "new_boot_inventory_removes_portable_park_pending_delete",
    "concurrent_different_spec_allocation_rejects_loser_atomically",
)
for _name in CONTRACTS:
    setattr(
        PostgresRoutingContracts,
        "test_" + _name,
        getattr(RoutingStoreTests, "test_" + _name),
    )
