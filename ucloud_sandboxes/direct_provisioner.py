from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import logging
from threading import Lock
from typing import Any

from .storage_native_migration import (
    StorageNativeMigrationError,
    MIGRATION_CONNECTION_POLICY_DISCONNECT,
    MIGRATION_CONNECTION_POLICY_NONE,
    StorageNativeMigration,
    StorageNativeMigrationStore,
)
from .direct_network import DirectNetworkManager
from .direct_oci import DirectOciConfigBuilder
from .direct_registry import (
    DirectRegistryError,
    DirectSandboxRegistration,
    DirectSandboxRegistry,
)
from .direct_warden import DirectRunscWarden, DirectWardenError
from .hibernation import HibernationState, hibernation_disk_reservation_mb
from .image_rootfs import DockerOverlay2RootfsStore, OverlayRootfsManager
from .sandbox import SandboxSpec
from .storage_native_daemon import (
    StorageNativeConflictError,
    StorageNativeNodeError,
    StorageVolumeOwner,
    StorageVolumeRecord,
    StorageVolumeState,
)


_LOG = logging.getLogger(__name__)
_MIB = 1024 * 1024


class DirectSandboxProvisioner:
    """Crash-durable owner of admission, quota, rootfs, and runsc provisioning."""

    def __init__(
        self,
        *,
        registry: DirectSandboxRegistry,
        image_store: DockerOverlay2RootfsStore,
        overlays: OverlayRootfsManager,
        oci: DirectOciConfigBuilder,
        warden: DirectRunscWarden,
        network_manager: DirectNetworkManager | None = None,
        storage_migrations: StorageNativeMigrationStore | None = None,
    ) -> None:
        self.registry = registry
        self.image_store = image_store
        self.overlays = overlays
        self.oci = oci
        self.warden = warden
        self.network_manager = network_manager
        self.storage_migrations = storage_migrations or StorageNativeMigrationStore(
            registry.path.parent / "storage-native-migrations"
        )
        self.runtime_compatibility_sha256 = (
            warden.config.runtime_fingerprint.node_compatibility_sha256
        )
        self.registry.bind_runtime_compatibility(self.runtime_compatibility_sha256)
        self._image_gc_state_guard = Lock()
        self._image_sweep_guard = Lock()
        self._image_gc_failure_generation = 1
        self._image_gc_reconciled_generation = 0
        self._validate_layout()

    def start(self) -> tuple[DirectSandboxRegistration, ...]:
        """Reconcile every registered incarnation."""
        registrations = self.registry.snapshot().records
        self.reconcile_image_cache(registrations)
        host_rules_ready = self.network_manager is not None
        if self.network_manager is not None:
            self.network_manager.reconcile()
        results: list[DirectSandboxRegistration] = []
        deletion_failures: list[tuple[str, Exception]] = []
        # Reclaim every durably deleted sandbox before advancing any pending
        # create. A node at its hard storage limit must still be able to free
        # capacity; registry ordering must never let planned work deadlock the
        # cleanup that makes that work admissible.
        for item in registrations:
            if item.phase != "deleting":
                continue
            try:
                self._delete_registration(item)
            except Exception as exc:
                # DELETING is the durable ownership fence. Keep serving
                # healthy sandboxes and let the service reconciler retry;
                # a transient cleanup failure must not require a node
                # restart or hide its still-reserved storage from metrics.
                deletion_failures.append((item.sandbox_id, exc))
        for item in registrations:
            if item.phase == "deleting":
                continue
            if (
                item.phase
                in {
                    "import_planned",
                    "importing",
                    "rootfs_ready",
                }
                and item.migration_id
            ):
                # Transfer bytes are deliberately not retained in the node
                # registry. The exact migration can be retried with the same
                # snapshot digest; never turn an interrupted import into a
                # newly-created sandbox.
                continue
            elif item.phase in {"import_ready", "moving_out"}:
                record = self.warden.inspect(item.to_direct_sandbox())
                if record is None:
                    raise DirectRegistryError(
                        "migration registration has no lifecycle journal"
                    )
                results.append(item)
            else:
                results.append(
                    self._advance(
                        item,
                        host_rules_ready=host_rules_ready,
                    )
                )
        if deletion_failures:
            first_id, first_error = deletion_failures[0]
            _LOG.warning(
                "deferred %d durable sandbox deletion(s); first=%s: %s",
                len(deletion_failures),
                first_id,
                first_error,
            )
        return tuple(results)

    def create(
        self,
        *,
        spec: SandboxSpec,
        sandbox_generation: int,
        operation_id: str,
    ) -> DirectSandboxRegistration:
        self._validate_spec(spec)
        # Resolve immutable image metadata and validate the full OCI translation
        # before persisting an operation or reserving node capacity.
        with self.image_store.operation_lease(spec.image) as image:
            registration = self.registry.plan(
                spec=spec,
                sandbox_generation=sandbox_generation,
                operation_id=operation_id,
                runtime_compatibility_sha256=self.runtime_compatibility_sha256,
            )
            if registration.phase == "planned":
                registration = self._prepare_quota(registration)
            return self._advance(registration, image=image)

    def reconcile(self, sandbox_id: str) -> DirectSandboxRegistration:
        registration = self.registry.get(sandbox_id)
        if registration is None:
            raise DirectRegistryError("direct sandbox registration is absent")
        return self._advance(registration)

    def stage_storage_native_import(
        self,
        migration: StorageNativeMigration,
        *,
        migration_id: str,
    ) -> tuple[DirectSandboxRegistration, StorageNativeMigration]:
        """Adopt a durable remote snapshot by its published storage identity."""

        portable = migration.manifest
        if (
            portable.runtime.node_compatibility_sha256
            != self.runtime_compatibility_sha256
        ):
            raise StorageNativeMigrationError(
                "storage-native migration belongs to another runtime compatibility"
            )
        self._validate_spec(portable.spec)
        with self.image_store.operation_lease(portable.spec.image) as image:
            return self._stage_storage_native_import_materialized(
                migration,
                migration_id=migration_id,
                image=image,
            )

    def _stage_storage_native_import_materialized(
        self,
        migration: StorageNativeMigration,
        *,
        migration_id: str,
        image,
    ) -> tuple[DirectSandboxRegistration, StorageNativeMigration]:
        portable = migration.manifest
        migration_sha256 = migration.sha256
        registration = self.registry.plan_import(
            spec=portable.spec,
            sandbox_generation=portable.sandbox_generation,
            operation_id=portable.create_operation_id,
            runtime_compatibility_sha256=self.runtime_compatibility_sha256,
            migration_id=migration_id,
            migration_sha256=migration_sha256,
        )
        if registration.phase == "import_planned":
            total_mb = self._quota_total_mb(registration)
            prepared = self.warden.storage.prepare_import(
                self._storage_owner(registration),
                publication=migration.publication,
                operation_id=f"quota-import:{migration_id}",
            )
            quota_path = self._require_storage_record(
                registration,
                prepared,
                total_mb=total_mb,
            )
            registration = self.registry.commit_import_quota(
                registration.sandbox_id,
                expected_revision=registration.revision,
                project_id=prepared.accounting_id,
                total_mb=total_mb,
                quota_path=quota_path,
            )
        config = self.oci.build(
            portable.spec,
            image,
            network_namespace_path=self._migration_network_namespace(
                registration,
                source_guest_ip=portable.source_guest_ip,
                connection_policy=portable.connection_policy,
            ),
        )
        if registration.phase == "owned":
            record = self.warden.inspect(registration.to_direct_sandbox())
            if record is None:
                raise DirectRegistryError("activated import has no lifecycle journal")
            stored = self.storage_migrations.load(migration_id)
            return registration, stored
        if (
            registration.migration_id != migration_id
            or registration.migration_sha256 != migration_sha256
        ):
            raise DirectRegistryError("destination already owns another migration")
        if registration.phase == "importing":
            local_manifest = self.storage_migrations.rebind_mounted_snapshot(
                migration,
                expected_runtime=replace(
                    self.warden.config.runtime_fingerprint,
                    rootfs_sha256=image.rootfs_identity_sha256,
                ),
                artifact_store=self.warden.artifacts,
                writable_incarnation=Path(registration.quota_path),
            )
            lease = self.overlays.prepare(
                sandbox_id=registration.sandbox_id,
                sandbox_generation=registration.sandbox_generation,
                image=image,
                config_template=config,
                spec_sha256=registration.spec_sha256,
                imported_parked=True,
            )
            registration = self.registry.commit_import_rootfs(
                registration.sandbox_id,
                expected_revision=registration.revision,
                image_id=lease.image.image_id,
                sandbox=lease.sandbox,
            )
        if registration.phase == "rootfs_ready" and registration.migration_id:
            sandbox = registration.to_direct_sandbox()
            record = self.warden.inspect(sandbox)
            if record is None:
                storage_record = self.warden._storage_record(sandbox)
                if storage_record.state.value == "published":
                    self.warden._mount_storage(
                        sandbox,
                        operation_id=f"import:{migration_id}:remount",
                    )
                    self.overlays.resume_sandbox(sandbox)
                    self.storage_migrations.rebind_mounted_snapshot(
                        migration,
                        expected_runtime=replace(
                            self.warden.config.runtime_fingerprint,
                            rootfs_sha256=image.rootfs_identity_sha256,
                        ),
                        artifact_store=self.warden.artifacts,
                        writable_incarnation=Path(registration.quota_path),
                    )
                local_manifest = self.warden.artifacts.load_complete(
                    sandbox_id=registration.sandbox_id,
                    sandbox_generation=registration.sandbox_generation,
                    hibernation_generation=portable.hibernation_generation,
                )
                record = self.warden.adopt_parked(sandbox, local_manifest)
            if record.state != HibernationState.PARKED:
                raise DirectRegistryError(
                    "storage-native destination is not durably parked"
                )
            published = self.warden.publish_storage_snapshot(
                sandbox,
                operation_id=f"import:{migration_id}:publish",
            )
            destination_migration = StorageNativeMigration(
                manifest=portable,
                publication=published.publication(),
            )
            self.storage_migrations.save(migration_id, destination_migration)
            registration = self.registry.commit_import_ready(
                registration.sandbox_id,
                expected_revision=registration.revision,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
            return registration, destination_migration
        if registration.phase == "import_ready":
            record = self.warden.inspect(registration.to_direct_sandbox())
            if record is None or record.state != HibernationState.PARKED:
                raise DirectRegistryError(
                    "storage-native import-ready sandbox is not durably parked"
                )
            return registration, self.storage_migrations.load(migration_id)
        raise DirectRegistryError(
            f"storage-native import cannot continue from {registration.phase}"
        )

    def activate_import(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectSandboxRegistration:
        registration = self.registry.get(sandbox_id)
        if registration is None:
            raise DirectRegistryError("migration destination is absent")
        if registration.phase == "owned":
            if (
                registration.migration_id != migration_id
                or registration.migration_sha256 != migration_sha256
            ):
                raise DirectRegistryError(
                    "activated sandbox belongs to another migration"
                )
            record = self.warden.inspect(registration.to_direct_sandbox())
            if record is None:
                raise DirectRegistryError("activated import has no lifecycle journal")
            return registration
        registration = self.registry.activate_import(
            sandbox_id,
            expected_revision=registration.revision,
            migration_id=migration_id,
            migration_sha256=migration_sha256,
        )
        record = self.warden.inspect(registration.to_direct_sandbox())
        if record is None or record.state != HibernationState.PARKED:
            raise DirectRegistryError("activated migration is not parked")
        return registration

    def abort_import(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> None:
        registration = self.registry.get(sandbox_id)
        if registration is None:
            return
        if registration.phase == "owned":
            raise DirectRegistryError(
                "an activated migration cannot be aborted as an import"
            )
        if registration.phase == "import_planned":
            self._drop_storage(
                registration,
                expected_project_id=None,
            )
            self.registry.abort_import_planned(
                sandbox_id,
                expected_revision=registration.revision,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
            return
        if registration.phase != "deleting":
            registration = self.registry.begin_delete_import(
                sandbox_id,
                expected_revision=registration.revision,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
        self._delete_registration(registration)

    def finalize_moved_source(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> None:
        registration = self.registry.get(sandbox_id)
        if registration is None:
            return
        if registration.phase == "moving_out":
            registration = self.registry.begin_delete_moved(
                sandbox_id,
                expected_revision=registration.revision,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
        if registration.phase != "deleting":
            raise DirectRegistryError(
                "source migration is not fenced for final deletion"
            )
        self._delete_registration(registration)

    def delete(self, sandbox_id: str) -> None:
        registration = self.registry.get(sandbox_id)
        if registration is None:
            return
        if registration.phase not in {"owned", "deleting"}:
            registration = self._advance(registration)
        if registration.phase == "owned":
            registration = self.registry.begin_delete(
                sandbox_id,
                expected_revision=registration.revision,
            )
        if registration.phase != "deleting":
            raise DirectRegistryError("direct sandbox could not enter deletion")
        self._delete_registration(registration)

    def _delete_registration(self, registration: DirectSandboxRegistration) -> None:
        sandbox_id = registration.sandbox_id
        sandbox = registration.to_direct_sandbox()
        self.warden.delete(sandbox)
        if self.network_manager is not None:
            self.network_manager.release(
                registration.sandbox_id,
                registration.sandbox_generation,
            )
        self.overlays.release_sandbox(sandbox)
        if registration.quota_project_id is None or registration.quota_total_mb is None:
            raise DirectRegistryError(
                "deleting direct registration lacks immutable quota identity"
            )
        self._drop_storage(
            registration,
            expected_project_id=registration.quota_project_id,
        )
        self.registry.commit_deleted(
            sandbox_id,
            sandbox_generation=registration.sandbox_generation,
            expected_revision=registration.revision,
        )
        self._collect_deleted_image(registration.image_id)

    @property
    def image_cache_reconciliation_pending(self) -> bool:
        with self._image_gc_state_guard:
            return (
                self._image_gc_reconciled_generation < self._image_gc_failure_generation
            )

    def reconcile_image_cache_if_pending(self) -> bool:
        with self._image_gc_state_guard:
            if (
                self._image_gc_reconciled_generation
                >= self._image_gc_failure_generation
            ):
                return False
        self.reconcile_image_cache()
        return True

    def _collect_deleted_image(self, image_id: str) -> None:
        try:
            self.image_store.collect_image(
                image_id,
                is_referenced=self.registry.references_image,
            )
        except Exception as exc:
            # Registry deletion is already durable. Cache reclamation is not
            # part of logical sandbox ownership, so report success and retain a
            # generation-fenced maintenance retry instead of making an
            # idempotent delete appear to fail.
            self._record_image_gc_failure()
            _LOG.warning(
                "deferred rootfs cache collection for deleted image %s: %s",
                image_id,
                exc,
            )

    def _record_image_gc_failure(self) -> int:
        with self._image_gc_state_guard:
            self._image_gc_failure_generation += 1
            return self._image_gc_failure_generation

    def reconcile_image_cache(
        self,
        registrations: tuple[DirectSandboxRegistration, ...] | None = None,
    ) -> None:
        """Collect only cache entries with no durable registry owner.

        ``registrations`` is the coherent startup root snapshot. The callback is
        deliberately evaluated again while GC owns each digest lock, closing
        the race with a provisioner that materialized before committing its
        registry record.
        """

        with self._image_sweep_guard:
            with self._image_gc_state_guard:
                failure_generation = self._image_gc_failure_generation
            if registrations is None:
                registrations = self.registry.snapshot().records
            try:
                self.image_store.reconcile_images(
                    (item.image_id for item in registrations if item.image_id),
                    is_referenced=self.registry.references_image,
                )
            except Exception:
                self._record_image_gc_failure()
                raise
            with self._image_gc_state_guard:
                self._image_gc_reconciled_generation = max(
                    self._image_gc_reconciled_generation,
                    failure_generation,
                )

    def _advance(
        self,
        registration: DirectSandboxRegistration,
        *,
        image=None,
        config: dict[str, Any] | None = None,
        host_rules_ready: bool = False,
    ) -> DirectSandboxRegistration:
        if (
            registration.runtime_compatibility_sha256
            != self.runtime_compatibility_sha256
        ):
            raise DirectRegistryError(
                "direct registration belongs to another runtime compatibility"
            )
        if registration.phase == "planned":
            registration = self._prepare_quota(registration)
        if registration.phase == "quota_ready" and image is None:
            with self.image_store.operation_lease(registration.spec.image) as image:
                return self._advance(
                    registration,
                    image=image,
                    config=config,
                    host_rules_ready=host_rules_ready,
                )
        network_namespace_path = self._network_namespace(
            registration,
            host_rules_ready=host_rules_ready,
        )
        if registration.phase == "quota_ready":
            assert image is not None
            config = config or self.oci.build(
                registration.spec,
                image,
                network_namespace_path=network_namespace_path,
            )
            # A crash can leave a mounted overlay only before rootfs_ready commits.
            # No runsc backend is allowed to exist at this phase.
            self.overlays.discard_unregistered(
                sandbox_id=registration.sandbox_id,
                sandbox_generation=registration.sandbox_generation,
            )
            lease = self.overlays.prepare(
                sandbox_id=registration.sandbox_id,
                sandbox_generation=registration.sandbox_generation,
                image=image,
                config_template=config,
                spec_sha256=registration.spec_sha256,
            )
            expected_path = Path(registration.quota_path)
            if lease.writable != expected_path:
                raise DirectWardenError(
                    "overlay writable path does not match quota ownership"
                )
            registration = self.registry.commit_rootfs(
                registration.sandbox_id,
                expected_revision=registration.revision,
                image_id=lease.image.image_id,
                sandbox=lease.sandbox,
            )
        if registration.phase == "rootfs_ready":
            sandbox = registration.to_direct_sandbox()
            # The public SDK defaults to an unprivileged OCI user and a
            # /workspace file API target. Establish that directory inside the
            # quota-accounted overlay before the backend can start.
            self.oci.prepare_workspace(
                sandbox.bundle / "rootfs",
                spec=registration.spec,
            )
            self.oci.prepare_network_files(
                sandbox.bundle / "rootfs",
                spec=registration.spec,
            )
            # Keep the init binary inside the quota-accounted rootfs. A bind
            # mount here is not executable under gVisor on production nodes.
            # This is deliberately replayed so startup repairs a crash between
            # committing the rootfs and creating the runsc backend.
            self.oci.install_init(
                sandbox.bundle / "rootfs",
                enabled=(
                    registration.spec.security.init
                    and not registration.spec.managed_process
                ),
            )
            self.oci.install_managed_init(
                sandbox.bundle / "rootfs",
                enabled=registration.spec.managed_process,
            )
            record = self.warden.inspect(sandbox)
            if record is None:
                # Covers a crash after runsc create but before journal commit.
                self.warden.discard_unjournaled(sandbox)
                record = self.warden.create(
                    sandbox,
                    operation_id=registration.operation_id,
                )
            elif record.state not in {
                HibernationState.RUNNING,
                HibernationState.PARKED,
            }:
                record = self.warden.reconcile(sandbox)
            if record.state not in {
                HibernationState.RUNNING,
                HibernationState.PARKED,
            }:
                raise DirectWardenError(
                    f"new direct sandbox settled in {record.state.value}"
                )
            registration = self.registry.commit_owned(
                registration.sandbox_id,
                expected_revision=registration.revision,
            )
        if registration.phase == "owned":
            record = self.warden.inspect(registration.to_direct_sandbox())
            if record is None:
                raise DirectWardenError("owned direct sandbox has no lifecycle journal")
            if record.state not in {
                HibernationState.RUNNING,
                HibernationState.PARKED,
            }:
                record = self.warden.reconcile(registration.to_direct_sandbox())
            if record.state not in {
                HibernationState.RUNNING,
                HibernationState.PARKED,
            }:
                raise DirectWardenError(
                    f"direct sandbox requires operator action: {record.state.value}"
                )
            return registration
        if registration.phase == "deleting":
            self.delete(registration.sandbox_id)
            raise DirectRegistryError(
                "direct sandbox was deleted during reconciliation"
            )
        raise DirectRegistryError(
            f"unsupported direct registration phase: {registration.phase}"
        )

    def _prepare_quota(
        self,
        registration: DirectSandboxRegistration,
    ) -> DirectSandboxRegistration:
        total_mb = self._quota_total_mb(registration)
        record = self.warden.storage.prepare_volume(
            self._storage_owner(registration),
            operation_id=registration.operation_id,
            virtual_size=total_mb * _MIB,
        )
        expected = self._require_storage_record(
            registration,
            record,
            total_mb=total_mb,
        )
        return self.registry.commit_quota(
            registration.sandbox_id,
            expected_revision=registration.revision,
            project_id=record.accounting_id,
            total_mb=total_mb,
            quota_path=expected,
        )

    def _drop_storage(
        self,
        registration: DirectSandboxRegistration,
        *,
        expected_project_id: int | None,
    ) -> None:
        owner = self._storage_owner(registration)
        total_mb = registration.quota_total_mb or self._quota_total_mb(registration)
        try:
            record = self.warden.storage.get_volume(owner.volume_id)
        except StorageNativeConflictError as exc:
            if expected_project_id is None:
                return
            raise StorageNativeNodeError(
                "storage-native quota owner is absent"
            ) from exc
        self._require_storage_record(
            registration,
            record,
            total_mb=total_mb,
            expected_project_id=expected_project_id,
        )
        if record.state == StorageVolumeState.DELETED:
            return
        delete_operation_id = f"quota-delete:{record.accounting_id}"
        if registration.migration_id:
            # A detached sandbox can later import the same generation and
            # volume ID on this worker. The storage journal deliberately keeps
            # the first deletion, so namespace a later imported incarnation by
            # its stable migration fence instead of reusing that operation ID
            # with a newer storage revision.
            delete_operation_id += f":{registration.migration_id}"
        self.warden.storage.delete_volume(
            owner,
            operation_id=delete_operation_id,
            expected_accounting_id=expected_project_id,
            expected_virtual_size=total_mb * _MIB,
        )

    def _require_storage_record(
        self,
        registration: DirectSandboxRegistration,
        record: StorageVolumeRecord,
        *,
        total_mb: int,
        expected_project_id: int | None = None,
    ) -> Path:
        if (
            record.owner != self._storage_owner(registration)
            or record.virtual_size != total_mb * _MIB
            or (
                expected_project_id is not None
                and record.accounting_id != expected_project_id
            )
        ):
            raise StorageNativeNodeError(
                "storage-native volume belongs to another quota owner"
            )
        expected = self.overlays.writable_root / record.volume_id
        if Path(record.mount_path) != expected:
            raise StorageNativeNodeError(
                "storage-native service returned an unexpected mount path"
            )
        if record.accounting_id <= 0:
            raise StorageNativeNodeError(
                "storage-native service returned an invalid accounting ID"
            )
        return expected

    @staticmethod
    def _storage_owner(
        registration: DirectSandboxRegistration,
    ) -> StorageVolumeOwner:
        return StorageVolumeOwner(
            volume_id=(
                f"{registration.sandbox_id}.sandbox-"
                f"{registration.sandbox_generation}"
            ),
            sandbox_id=registration.sandbox_id,
            sandbox_generation=registration.sandbox_generation,
        )

    @staticmethod
    def _quota_total_mb(registration: DirectSandboxRegistration) -> int:
        memory_mb = registration.spec.memory_mb
        disk_mb = registration.spec.disk_mb
        if memory_mb is None or disk_mb is None:
            raise DirectRegistryError(
                "direct registration lacks explicit memory and disk limits"
            )
        return hibernation_disk_reservation_mb(
            memory_mb=memory_mb,
            writable_disk_mb=disk_mb,
        )

    def _validate_layout(self) -> None:
        config = self.warden.config
        if (
            self.overlays.bundle_root != config.bundle_root
            or self.overlays.writable_root != config.memory_root
            or not self.overlays.require_precreated_writable
            or self.warden.rootfs_lifecycle is not self.overlays
        ):
            raise ValueError(
                "direct provisioner requires the unified quota-owned runtime layout"
            )

    def _validate_spec(self, spec: SandboxSpec) -> None:
        spec.validate()
        if spec.memory_mb is None or spec.disk_mb is None:
            raise ValueError(
                "direct sandboxes require explicit memory_mb and disk_mb limits"
            )
        expected_network = "none" if spec.network == "none" else "sandbox"
        if expected_network != self.warden.config.network:
            raise ValueError(
                "sandbox network does not match the node-wide direct runtime mode"
            )
        if expected_network == "sandbox" and self.network_manager is None:
            raise ValueError("direct sandbox networking has no node network manager")
        if expected_network == "none" and self.network_manager is not None:
            raise ValueError("network=none cannot use the node sandbox network manager")

    def _network_namespace(
        self,
        registration: DirectSandboxRegistration,
        *,
        host_rules_ready: bool = False,
    ) -> Path | None:
        if registration.spec.network == "none":
            return None
        if self.network_manager is None:
            raise DirectRegistryError("direct sandbox network manager is absent")
        return self.network_manager.ensure(
            registration.sandbox_id,
            registration.sandbox_generation,
            host_rules_ready=host_rules_ready,
        ).namespace_path

    def ensure_network(
        self,
        registration: DirectSandboxRegistration,
    ) -> Path | None:
        """Materialize external network wiring before create or restore."""
        return self._network_namespace(registration)

    def _migration_network_namespace(
        self,
        registration: DirectSandboxRegistration,
        *,
        source_guest_ip: str | None,
        connection_policy: str,
    ) -> Path | None:
        if registration.spec.network == "none":
            if (
                source_guest_ip is not None
                or connection_policy != MIGRATION_CONNECTION_POLICY_NONE
            ):
                raise StorageNativeMigrationError(
                    "network=none migration carries network reconnect state"
                )
            return None
        if self.network_manager is None:
            raise StorageNativeMigrationError(
                "networked migration has no destination network manager"
            )
        if (
            source_guest_ip is None
            or connection_policy != MIGRATION_CONNECTION_POLICY_DISCONNECT
        ):
            raise StorageNativeMigrationError(
                "networked migration does not require a safe reconnect"
            )
        lease = self.network_manager.ensure(
            registration.sandbox_id,
            registration.sandbox_generation,
            avoid_guest_ips=(source_guest_ip,),
        )
        if lease.guest_ip == source_guest_ip:
            raise StorageNativeMigrationError(
                "migration destination retained the source guest IP"
            )
        return lease.namespace_path
