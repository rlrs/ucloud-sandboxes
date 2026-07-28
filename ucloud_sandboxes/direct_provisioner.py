from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .direct_oci import DirectOciConfigBuilder
from .direct_registry import (
    DirectRegistryError,
    DirectSandboxRegistration,
    DirectSandboxRegistry,
)
from .direct_warden import DirectRunscWarden, DirectWardenError
from .hibernation import (
    HibernationCapacityError,
    HibernationDiskLedger,
    HibernationDiskReservation,
    HibernationState,
)
from .image_rootfs import DockerRootfsStore, OverlayRootfsManager
from .runtime_identity import (
    NodeRuntimeIdentity,
    NodeRuntimeIdentityStore,
)
from .sandbox import HibernationQuotaBackend, SandboxSpec


@dataclass(frozen=True)
class DirectProvisioningResult:
    registration: DirectSandboxRegistration
    lifecycle_state: HibernationState


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
    ) -> None:
        self.identity_store = identity_store
        self.registry = registry
        self.disk_ledger = disk_ledger
        self.quota_backend = quota_backend
        self.image_store = image_store
        self.overlays = overlays
        self.oci = oci
        self.warden = warden
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
        for item in self.registry.list():
            if item.phase == "deleting":
                self.delete(item.sandbox_id)
            else:
                results.append(self.reconcile(item.sandbox_id))
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
        config = self.oci.build(spec, image)
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
        return self._advance(registration, image=image, config=config)

    def reconcile(self, sandbox_id: str) -> DirectProvisioningResult:
        self.identity_store.bind(self.identity)
        registration = self.registry.get(sandbox_id)
        if registration is None:
            raise DirectRegistryError("direct sandbox registration is absent")
        return self._advance(registration)

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
        sandbox = registration.to_direct_sandbox()
        self.warden.delete(sandbox)
        self.overlays.release_sandbox(sandbox)
        reservation = self._reservation_for(registration)
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
        if registration.phase == "quota_ready":
            image = image or self.image_store.materialize(registration.spec.image)
            config = config or self.oci.build(registration.spec, image)
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
            # Keep the init binary inside the quota-accounted rootfs. A bind
            # mount here is not executable under gVisor on production nodes.
            # This is deliberately replayed so startup repairs a crash between
            # committing the rootfs and creating the runsc backend.
            self.oci.install_init(
                sandbox.bundle / "rootfs",
                enabled=registration.spec.security.init,
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
    ) -> HibernationDiskReservation:
        assert registration.spec.memory_mb is not None
        assert registration.spec.disk_mb is not None
        return self.disk_ledger.reserve(
            sandbox_id=registration.sandbox_id,
            sandbox_generation=registration.sandbox_generation,
            memory_mb=registration.spec.memory_mb,
            writable_disk_mb=registration.spec.disk_mb,
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
            if registration.phase != "planned" and reservation is None:
                raise DirectRegistryError(
                    "registered direct sandbox is missing disk admission"
                )
            if registration.phase in {
                "quota_ready",
                "rootfs_ready",
                "owned",
                "deleting",
            } and quota is None:
                raise DirectRegistryError(
                    "registered direct sandbox is missing physical quota"
                )
            if quota is not None:
                if reservation is None:
                    raise DirectRegistryError(
                        "physical quota has no logical disk reservation"
                    )
                if (
                    quota["project_id"] != reservation.project_id
                    or quota["hard_limit_mb"] != reservation.total_mb
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
        if spec.network != "none":
            raise ValueError(
                "direct bridge networking is not qualified on this runtime"
            )
