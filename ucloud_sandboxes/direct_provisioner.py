from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
import logging
import shutil
import time
from typing import Any

from .direct_migration import (
    DirectMigrationArchiveStore,
    DirectMigrationError,
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
from .hibernation import (
    HIBERNATION_FIXED_OVERHEAD_MB,
    HibernationCapacityError,
    HibernationDiskLedger,
    HibernationDiskReservation,
    HibernationState,
    hibernation_memory_backing_reservation_mb,
)
from .image_rootfs import DockerRootfsStore, OverlayRootfsManager
from .runtime_identity import (
    NodeRuntimeIdentity,
    NodeRuntimeIdentityStore,
)
from .sandbox import HibernationQuotaBackend, SandboxSpec
from .storage_native_quota import StorageNativeQuotaBackend
from .storage_native_registry import (
    PublishedStorageLayer,
    StorageSnapshotPublication,
)


_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class DirectProvisioningResult:
    registration: DirectSandboxRegistration
    lifecycle_state: HibernationState


@dataclass(frozen=True)
class DirectStorageNativeImportResult:
    provisioning: DirectProvisioningResult
    migration: StorageNativeMigration


class DirectSandboxProvisioner:
    """Crash-durable owner of admission, quota, rootfs, and runsc provisioning."""

    def __init__(
        self,
        *,
        identity_store: NodeRuntimeIdentityStore,
        registry: DirectSandboxRegistry,
        disk_ledger: HibernationDiskLedger,
        quota_backend: HibernationQuotaBackend,
        image_store: DockerRootfsStore,
        overlays: OverlayRootfsManager,
        oci: DirectOciConfigBuilder,
        warden: DirectRunscWarden,
        network_manager: DirectNetworkManager | None = None,
        migration_archives: DirectMigrationArchiveStore | None = None,
        storage_migrations: StorageNativeMigrationStore | None = None,
    ) -> None:
        self.identity_store = identity_store
        self.registry = registry
        self.disk_ledger = disk_ledger
        self.quota_backend = quota_backend
        self.image_store = image_store
        self.overlays = overlays
        self.oci = oci
        self.warden = warden
        self.network_manager = network_manager
        self.migration_archives = (
            migration_archives or DirectMigrationArchiveStore()
        )
        self.storage_migrations = storage_migrations or StorageNativeMigrationStore(
            registry.path.parent / "storage-native-migrations"
        )
        self.identity = NodeRuntimeIdentity.from_fingerprint(
            warden.config.runtime_fingerprint
        )
        self._validate_layout()

    def start(self) -> tuple[DirectProvisioningResult, ...]:
        """Bind node identity, audit all owners, and reconcile every incarnation."""
        self.identity_store.bind(self.identity)
        self.image_store.reconcile_export_containers()
        self._audit_ownership()
        results: list[DirectProvisioningResult] = []
        deletion_failures: list[tuple[str, Exception]] = []
        for item in self.registry.list():
            if item.phase == "deleting":
                try:
                    self.delete(item.sandbox_id)
                except Exception as exc:
                    # DELETING is the durable ownership fence. Keep serving
                    # healthy sandboxes and let the service reconciler retry;
                    # a transient cleanup failure must not require a node
                    # restart or hide its still-reserved storage from metrics.
                    deletion_failures.append((item.sandbox_id, exc))
            elif item.phase in {
                "import_planned",
                "importing",
                "rootfs_ready",
            } and item.migration_id:
                # Transfer bytes are deliberately not retained in the node
                # registry. The exact migration can be retried with the same
                # archive digest; never turn an interrupted import into a
                # newly-created sandbox.
                continue
            elif item.phase in {"import_ready", "moving_out"}:
                record = self.warden.inspect(item.to_direct_sandbox())
                if record is None:
                    raise DirectRegistryError(
                        "migration registration has no lifecycle journal"
                    )
                results.append(DirectProvisioningResult(item, record.state))
            else:
                results.append(self.reconcile(item.sandbox_id))
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
    ) -> DirectProvisioningResult:
        self.identity_store.bind(self.identity)
        self._validate_spec(spec)
        # Resolve immutable image metadata and validate the full OCI translation
        # before persisting an operation or reserving node capacity.
        image = self.image_store.materialize(spec.image)
        registration = self.registry.plan(
            spec=spec,
            sandbox_generation=sandbox_generation,
            operation_id=operation_id,
            runtime_identity_sha256=self.identity.digest,
        )
        if registration.phase == "planned":
            try:
                reservation = self._reserve(registration)
            except HibernationCapacityError:
                self.registry.abort_planned(
                    spec.id,
                    expected_revision=registration.revision,
                )
                raise
            registration = self._prepare_quota(registration, reservation)
        return self._advance(registration, image=image)

    def reconcile(self, sandbox_id: str) -> DirectProvisioningResult:
        self.identity_store.bind(self.identity)
        registration = self.registry.get(sandbox_id)
        if registration is None:
            raise DirectRegistryError("direct sandbox registration is absent")
        return self._advance(registration)

    def stage_import(
        self,
        archive_path: Path,
        *,
        expected_sha256: str,
        migration_id: str,
    ) -> DirectProvisioningResult:
        """Import a parked sandbox but keep it unavailable until route CAS."""
        self.identity_store.bind(self.identity)
        portable = self.migration_archives.read_manifest(
            archive_path,
            expected_sha256=expected_sha256,
        )
        if portable.runtime_identity != self.identity:
            raise DirectMigrationError(
                "migration archive belongs to another node runtime identity"
            )
        self._validate_spec(portable.spec)
        image = self.image_store.materialize(portable.spec.image)
        registration = self.registry.plan_import(
            spec=portable.spec,
            sandbox_generation=portable.sandbox_generation,
            operation_id=portable.create_operation_id,
            runtime_identity_sha256=self.identity.digest,
            migration_id=migration_id,
            migration_sha256=expected_sha256,
        )
        if registration.phase == "import_planned":
            try:
                reservation = self._reserve(
                    registration,
                    allow_released_generation=True,
                )
            except HibernationCapacityError:
                self.registry.abort_import_planned(
                    portable.sandbox_id,
                    expected_revision=registration.revision,
                    migration_id=migration_id,
                    migration_sha256=expected_sha256,
                    retire=False,
                )
                raise
            prepared = self.quota_backend.prepare(reservation)
            quota_path = Path(str(prepared["path"]))
            if quota_path != self._quota_path(registration):
                raise DirectRegistryError(
                    "quota helper returned a different sandbox incarnation"
                )
            registration = self.registry.commit_import_quota(
                registration.sandbox_id,
                expected_revision=registration.revision,
                project_id=reservation.project_id,
                total_mb=reservation.total_mb,
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
            return DirectProvisioningResult(registration, record.state)
        if registration.migration_id != migration_id or (
            registration.migration_sha256 != expected_sha256
        ):
            raise DirectRegistryError(
                "destination already owns another migration"
            )
        if registration.phase == "importing":
            self._reset_interrupted_import(registration)
            _, local_manifest = self.migration_archives.import_archive(
                archive_path,
                expected_sha256=expected_sha256,
                expected_runtime_identity=self.identity,
                expected_runtime=replace(
                    self.warden.config.runtime_fingerprint,
                    rootfs_sha256=image.rootfs_identity_sha256,
                ),
                artifact_store=self.warden.artifacts,
                writable_incarnation=Path(registration.quota_path),
                portable_manifest=portable,
            )
            lease = self.overlays.prepare(
                sandbox_id=registration.sandbox_id,
                sandbox_generation=registration.sandbox_generation,
                image_ref=registration.spec.image,
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
            local_manifest = self.warden.artifacts.load_complete(
                sandbox_id=registration.sandbox_id,
                sandbox_generation=registration.sandbox_generation,
                hibernation_generation=portable.hibernation_generation,
            )
            record = self.warden.adopt_parked(sandbox, local_manifest)
            registration = self.registry.commit_import_ready(
                registration.sandbox_id,
                expected_revision=registration.revision,
                migration_id=migration_id,
                migration_sha256=expected_sha256,
            )
            return DirectProvisioningResult(registration, record.state)
        if registration.phase == "import_ready":
            record = self.warden.inspect(registration.to_direct_sandbox())
            if record is None or record.state != HibernationState.PARKED:
                raise DirectRegistryError(
                    "import-ready sandbox is not durably parked"
                )
            return DirectProvisioningResult(registration, record.state)
        raise DirectRegistryError(
            f"migration import cannot continue from {registration.phase}"
        )

    def stage_storage_native_import(
        self,
        migration: StorageNativeMigration,
        *,
        migration_id: str,
    ) -> DirectStorageNativeImportResult:
        """Adopt a durable remote snapshot without transferring an archive."""

        if not isinstance(self.quota_backend, StorageNativeQuotaBackend):
            raise DirectMigrationError(
                "storage-native migration requires the storage-native quota backend"
            )
        portable = migration.manifest
        if portable.runtime_identity != self.identity:
            raise DirectMigrationError(
                "storage-native migration belongs to another runtime identity"
            )
        self._validate_spec(portable.spec)
        image = self.image_store.materialize(portable.spec.image)
        migration_sha256 = migration.sha256
        registration = self.registry.plan_import(
            spec=portable.spec,
            sandbox_generation=portable.sandbox_generation,
            operation_id=portable.create_operation_id,
            runtime_identity_sha256=self.identity.digest,
            migration_id=migration_id,
            migration_sha256=migration_sha256,
        )
        if registration.phase == "import_planned":
            try:
                reservation = self._reserve(
                    registration,
                    allow_released_generation=True,
                )
            except HibernationCapacityError:
                self.registry.abort_import_planned(
                    portable.sandbox_id,
                    expected_revision=registration.revision,
                    migration_id=migration_id,
                    migration_sha256=migration_sha256,
                    retire=False,
                )
                raise
            prepared = self.quota_backend.prepare_import(
                reservation,
                migration.publication,
                migration_id=migration_id,
            )
            quota_path = Path(str(prepared["path"]))
            if quota_path != self._quota_path(registration):
                raise DirectRegistryError(
                    "storage-native import returned another sandbox incarnation"
                )
            registration = self.registry.commit_import_quota(
                registration.sandbox_id,
                expected_revision=registration.revision,
                project_id=reservation.project_id,
                total_mb=reservation.total_mb,
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
            return DirectStorageNativeImportResult(
                DirectProvisioningResult(registration, record.state),
                stored,
            )
        if (
            registration.migration_id != migration_id
            or registration.migration_sha256 != migration_sha256
        ):
            raise DirectRegistryError("destination already owns another migration")
        if registration.phase == "importing":
            local_manifest = self.storage_migrations.rebind_mounted_snapshot(
                migration,
                expected_runtime_identity=self.identity,
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
                image_ref=registration.spec.image,
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
                if storage_record.get("state") == "published":
                    self.warden._mount_storage(
                        sandbox,
                        operation_id=f"import:{migration_id}:remount",
                    )
                    self.overlays.resume_sandbox(sandbox)
                    self.storage_migrations.rebind_mounted_snapshot(
                        migration,
                        expected_runtime_identity=self.identity,
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
                publication=self._publication_from_storage_record(published),
            )
            self.storage_migrations.save(migration_id, destination_migration)
            registration = self.registry.commit_import_ready(
                registration.sandbox_id,
                expected_revision=registration.revision,
                migration_id=migration_id,
                migration_sha256=migration_sha256,
            )
            return DirectStorageNativeImportResult(
                DirectProvisioningResult(registration, record.state),
                destination_migration,
            )
        if registration.phase == "import_ready":
            record = self.warden.inspect(registration.to_direct_sandbox())
            if record is None or record.state != HibernationState.PARKED:
                raise DirectRegistryError(
                    "storage-native import-ready sandbox is not durably parked"
                )
            return DirectStorageNativeImportResult(
                DirectProvisioningResult(registration, record.state),
                self.storage_migrations.load(migration_id),
            )
        raise DirectRegistryError(
            f"storage-native import cannot continue from {registration.phase}"
        )

    @staticmethod
    def _publication_from_storage_record(
        record: dict[str, object],
    ) -> StorageSnapshotPublication:
        layers = record.get("published_layers")
        if not isinstance(layers, list):
            raise DirectRegistryError(
                "published storage-native volume has invalid layers"
            )
        return StorageSnapshotPublication(
            manifest_digest=str(record.get("published_manifest_digest") or ""),
            tag=str(record.get("published_tag") or ""),
            repository=str(record.get("published_repository") or ""),
            repo_blob_url=str(record.get("published_repo_blob_url") or ""),
            virtual_size=int(record.get("virtual_size") or 0),
            layers=tuple(PublishedStorageLayer.from_dict(layer) for layer in layers),
        )

    def activate_import(
        self,
        sandbox_id: str,
        *,
        migration_id: str,
        migration_sha256: str,
    ) -> DirectProvisioningResult:
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
            return DirectProvisioningResult(registration, record.state)
        registration = self.registry.activate_import(
            sandbox_id,
            expected_revision=registration.revision,
            migration_id=migration_id,
            migration_sha256=migration_sha256,
        )
        record = self.warden.inspect(registration.to_direct_sandbox())
        if record is None or record.state != HibernationState.PARKED:
            raise DirectRegistryError("activated migration is not parked")
        return DirectProvisioningResult(registration, record.state)

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
            matching = [
                item
                for item in self.disk_ledger.inventory().reservations
                if item.sandbox_id == registration.sandbox_id
                and item.sandbox_generation
                == registration.sandbox_generation
            ]
            if len(matching) > 1:
                raise DirectRegistryError(
                    "import plan owns multiple disk reservations"
                )
            if matching:
                self.quota_backend.drop(matching[0])
                self.disk_ledger.release(
                    sandbox_id=registration.sandbox_id,
                    sandbox_generation=registration.sandbox_generation,
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
        self.identity_store.bind(self.identity)
        registration = self.registry.get(sandbox_id)
        if registration is None:
            return
        if registration.phase not in {"owned", "deleting"}:
            registration = self._advance(registration).registration
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
        reservation = self._deletion_reservation_for(registration)
        self.quota_backend.drop(reservation)
        # Logical node capacity is released only after the physical quota tree
        # is gone. Replays after this boundary are fenced by the ledger tombstone.
        self.disk_ledger.release(
            sandbox_id=registration.sandbox_id,
            sandbox_generation=registration.sandbox_generation,
        )
        self.registry.commit_deleted(
            sandbox_id,
            sandbox_generation=registration.sandbox_generation,
            expected_revision=registration.revision,
        )

    def _deletion_reservation_for(
        self,
        registration: DirectSandboxRegistration,
    ) -> HibernationDiskReservation:
        """Recover cleanup identity across the ledger-release crash boundary.

        Physical quota is removed before logical admission is released. A
        crash after the ledger tombstone but before registry completion must
        still be able to replay quota deletion and commit the DELETING record.
        The immutable registry record contains every field needed to recreate
        that cleanup capability, and the total is checked against the original
        quota commitment before it is used.
        """

        matching = [
            item
            for item in self.disk_ledger.inventory().reservations
            if item.sandbox_id == registration.sandbox_id
            and item.sandbox_generation == registration.sandbox_generation
        ]
        if len(matching) > 1:
            raise DirectRegistryError(
                "deleting direct registration owns multiple disk reservations"
            )
        if matching:
            return matching[0]
        if registration.phase != "deleting":
            raise DirectRegistryError(
                "direct registration does not have exactly one disk reservation"
            )
        if (
            registration.quota_project_id is None
            or registration.quota_total_mb is None
            or registration.spec.memory_mb is None
            or registration.spec.disk_mb is None
        ):
            raise DirectRegistryError(
                "deleting direct registration lacks immutable quota identity"
            )
        reservation = HibernationDiskReservation(
            sandbox_id=registration.sandbox_id,
            sandbox_generation=registration.sandbox_generation,
            project_id=registration.quota_project_id,
            memory_mb=registration.spec.memory_mb,
            writable_disk_mb=registration.spec.disk_mb,
            memory_backing_mb=hibernation_memory_backing_reservation_mb(
                registration.spec.memory_mb
            ),
            private_pages_mb=registration.spec.memory_mb,
            fixed_overhead_mb=HIBERNATION_FIXED_OVERHEAD_MB,
            created_ns=max(1, registration.created_ns or time.time_ns()),
        )
        if reservation.total_mb != registration.quota_total_mb:
            raise DirectRegistryError(
                "deleting direct registration quota identity changed"
            )
        return reservation

    def _reset_interrupted_import(
        self,
        registration: DirectSandboxRegistration,
    ) -> None:
        """Remove only non-authoritative state owned by an IMPORTING record."""
        if registration.phase != "importing":
            raise DirectRegistryError("only an importing registration may reset")
        self.overlays.discard_unregistered(
            sandbox_id=registration.sandbox_id,
            sandbox_generation=registration.sandbox_generation,
        )
        writable = Path(registration.quota_path)
        expected = self._quota_path(registration)
        if writable != expected or writable.parent != self.overlays.writable_root:
            raise DirectRegistryError("import reset escaped its quota incarnation")
        for child in tuple(writable.iterdir()):
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()

    def _advance(
        self,
        registration: DirectSandboxRegistration,
        *,
        image=None,
        config: dict[str, Any] | None = None,
    ) -> DirectProvisioningResult:
        if registration.runtime_identity_sha256 != self.identity.digest:
            raise DirectRegistryError(
                "direct registration belongs to another runtime identity"
            )
        if registration.phase == "planned":
            reservation = self._reserve(registration)
            registration = self._prepare_quota(registration, reservation)
        network_namespace_path = self._network_namespace(registration)
        if registration.phase == "quota_ready":
            image = image or self.image_store.materialize(registration.spec.image)
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
                image_ref=registration.spec.image,
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
                raise DirectWardenError(
                    "owned direct sandbox has no lifecycle journal"
                )
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
            return DirectProvisioningResult(registration, record.state)
        if registration.phase == "deleting":
            self.delete(registration.sandbox_id)
            raise DirectRegistryError("direct sandbox was deleted during reconciliation")
        raise DirectRegistryError(
            f"unsupported direct registration phase: {registration.phase}"
        )

    def _prepare_quota(
        self,
        registration: DirectSandboxRegistration,
        reservation: HibernationDiskReservation,
    ) -> DirectSandboxRegistration:
        payload = self.quota_backend.prepare(reservation)
        expected = self._quota_path(registration)
        if Path(str(payload.get("path", ""))) != expected:
            raise DirectWardenError(
                "quota helper prepared an unexpected direct sandbox path"
            )
        if (
            payload.get("project_id") != reservation.project_id
            or payload.get("hard_limit_mb") != reservation.total_mb
        ):
            raise DirectWardenError(
                "quota helper did not enforce the full direct sandbox reservation"
            )
        return self.registry.commit_quota(
            registration.sandbox_id,
            expected_revision=registration.revision,
            project_id=reservation.project_id,
            total_mb=reservation.total_mb,
            quota_path=expected,
        )

    def _reserve(
        self,
        registration: DirectSandboxRegistration,
        *,
        allow_released_generation: bool = False,
    ) -> HibernationDiskReservation:
        assert registration.spec.memory_mb is not None
        assert registration.spec.disk_mb is not None
        return self.disk_ledger.reserve(
            sandbox_id=registration.sandbox_id,
            sandbox_generation=registration.sandbox_generation,
            memory_mb=registration.spec.memory_mb,
            writable_disk_mb=registration.spec.disk_mb,
            allow_released_generation=allow_released_generation,
        )

    def _reservation_for(
        self,
        registration: DirectSandboxRegistration,
    ) -> HibernationDiskReservation:
        matching = [
            item
            for item in self.disk_ledger.inventory().reservations
            if item.sandbox_id == registration.sandbox_id
            and item.sandbox_generation == registration.sandbox_generation
        ]
        if len(matching) != 1:
            raise DirectRegistryError(
                "direct registration does not have exactly one disk reservation"
            )
        reservation = matching[0]
        if registration.quota_project_id not in {None, reservation.project_id}:
            raise DirectRegistryError("direct registration quota project mismatches ledger")
        if registration.quota_total_mb not in {None, reservation.total_mb}:
            raise DirectRegistryError("direct registration quota limit mismatches ledger")
        return reservation

    def _quota_path(self, registration: DirectSandboxRegistration) -> Path:
        return self.overlays.writable_root / (
            f"{registration.sandbox_id}.sandbox-{registration.sandbox_generation}"
        )

    def _audit_ownership(self) -> None:
        registrations = {
            (item.sandbox_id, item.sandbox_generation): item
            for item in self.registry.list()
        }
        reservations = {
            (item.sandbox_id, item.sandbox_generation): item
            for item in self.disk_ledger.inventory().reservations
        }
        quota_inventory = {
            (str(item["sandbox_id"]), int(item["sandbox_generation"])): item
            for item in self.quota_backend.inventory()
        }
        if set(reservations) - set(registrations):
            raise DirectRegistryError(
                "disk ledger contains an owner absent from the direct registry"
            )
        if set(quota_inventory) - set(registrations):
            raise DirectRegistryError(
                "quota backend contains an owner absent from the direct registry"
            )
        for identity, registration in registrations.items():
            if registration.runtime_identity_sha256 != self.identity.digest:
                raise DirectRegistryError(
                    "direct registry contains another runtime identity"
                )
            reservation = reservations.get(identity)
            quota = quota_inventory.get(identity)
            if registration.phase not in {"planned", "import_planned"} and (
                reservation is None
            ) and registration.phase != "deleting":
                raise DirectRegistryError(
                    "registered direct sandbox is missing disk admission"
                )
            if registration.phase in {
                "quota_ready",
                "importing",
                "rootfs_ready",
                "import_ready",
                "owned",
                "moving_out",
                "deleting",
            } and quota is None:
                if registration.phase != "deleting":
                    raise DirectRegistryError(
                        "registered direct sandbox is missing physical quota"
                    )
            if quota is not None:
                ownership = reservation
                if ownership is None and registration.phase == "deleting":
                    ownership = self._deletion_reservation_for(registration)
                if ownership is None:
                    raise DirectRegistryError(
                        "physical quota has no logical disk reservation"
                    )
                if (
                    quota["project_id"] != ownership.project_id
                    or quota["hard_limit_mb"] != ownership.total_mb
                    or Path(quota["path"]) != self._quota_path(registration)
                ):
                    raise DirectRegistryError(
                        "direct sandbox physical quota ownership is inconsistent"
                    )

    def _validate_layout(self) -> None:
        config = self.warden.config
        if (
            self.overlays.bundle_root != config.bundle_root
            or self.overlays.writable_root != config.memory_root
            or config.memory_root != config.artifact_root
            or not self.overlays.require_precreated_writable
            or config.remove_memory_directory_on_delete
        ):
            raise ValueError(
                "direct provisioner requires the unified quota-owned runtime layout"
            )

    def _validate_spec(self, spec: SandboxSpec) -> None:
        if spec.forkable:
            raise ValueError("fork is deferred from the direct runtime")
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
    ) -> Path | None:
        if registration.spec.network == "none":
            return None
        if self.network_manager is None:
            raise DirectRegistryError("direct sandbox network manager is absent")
        return self.network_manager.ensure(
            registration.sandbox_id,
            registration.sandbox_generation,
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
                raise DirectMigrationError(
                    "network=none migration carries network reconnect state"
                )
            return None
        if self.network_manager is None:
            raise DirectMigrationError(
                "networked migration has no destination network manager"
            )
        if (
            source_guest_ip is None
            or connection_policy != MIGRATION_CONNECTION_POLICY_DISCONNECT
        ):
            raise DirectMigrationError(
                "networked migration does not require a safe reconnect"
            )
        lease = self.network_manager.ensure(
            registration.sandbox_id,
            registration.sandbox_generation,
            avoid_guest_ips=(source_guest_ip,),
        )
        if lease.guest_ip == source_guest_ip:
            raise DirectMigrationError(
                "migration destination retained the source guest IP"
            )
        return lease.namespace_path
