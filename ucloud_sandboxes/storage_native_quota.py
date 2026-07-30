from __future__ import annotations

from pathlib import Path
from typing import Any

from .hibernation import HibernationDiskReservation
from .hibernation import HibernationDiskLedger
from .storage_native_daemon import (
    StorageNativeConflictError,
    StorageNativeNodeClient,
    StorageNativeNodeError,
)
from .storage_native_registry import StorageSnapshotPublication


MIB = 1024 * 1024
_IDENTITY_LEDGER_CAPACITY_MB = 1 << 50


class StorageNativeReservationLedger(HibernationDiskLedger):
    """Allocate stable accounting IDs; the storage daemon owns hard admission.

    Published volumes deliberately remain registered while charging zero local
    bytes. Reusing the legacy ledger's physical-capacity sum would pin every
    parked sandbox to its old node, so this ledger is identity-only. Every
    create/mount still fails closed in StorageNativeNodeService against the
    real hard byte capacity.
    """

    def __init__(
        self,
        path: Path,
        *,
        first_project_id: int = 200_000,
    ) -> None:
        super().__init__(
            path,
            capacity_mb=_IDENTITY_LEDGER_CAPACITY_MB,
            safety_headroom_mb=0,
            first_project_id=first_project_id,
        )


class StorageNativeQuotaBackend:
    """Expose mounted storage-native volumes through the direct quota contract."""

    def __init__(
        self,
        client: StorageNativeNodeClient,
        *,
        mount_root: Path,
    ) -> None:
        if not mount_root.is_absolute():
            raise ValueError("storage-native mount root must be absolute")
        self.client = client
        self.mount_root = mount_root

    def prepare(
        self,
        reservation: HibernationDiskReservation,
    ) -> dict[str, Any]:
        volume_id = self._volume_id(reservation)
        try:
            result = self.client.get_volume(volume_id)
        except StorageNativeConflictError:
            result = self.client.create_volume(
                sandbox_id=reservation.sandbox_id,
                sandbox_generation=reservation.sandbox_generation,
                volume_id=volume_id,
                operation_id=f"quota-create:{reservation.project_id}",
                virtual_size=reservation.total_mb * MIB,
                accounting_id=reservation.project_id,
            )
        record = self._record(result)
        self._require_owner(record, reservation)
        if record["state"] != "mounted":
            raise StorageNativeNodeError(
                f"prepared storage-native volume is {record['state']}, not mounted"
            )
        expected_path = self.mount_root / volume_id
        if Path(record["mount_path"]) != expected_path:
            raise StorageNativeNodeError(
                "storage-native service returned an unexpected mount path"
            )
        return self._ready_payload(record, reservation)

    def drop(
        self,
        reservation: HibernationDiskReservation,
    ) -> dict[str, Any]:
        volume_id = self._volume_id(reservation)
        try:
            result = self.client.get_volume(volume_id)
        except StorageNativeConflictError:
            return self._absent_payload(reservation, removed=False)
        record = self._record(result)
        self._require_owner(record, reservation)
        if record["state"] == "deleted":
            return self._absent_payload(reservation, removed=False)
        self.client.delete_volume(
            sandbox_id=reservation.sandbox_id,
            sandbox_generation=reservation.sandbox_generation,
            volume_id=volume_id,
            operation_id=(
                f"quota-delete:{reservation.project_id}:{int(record['revision'])}"
            ),
            expected_revision=int(record["revision"]),
        )
        return self._absent_payload(reservation, removed=True)

    def prepare_import(
        self,
        reservation: HibernationDiskReservation,
        publication: StorageSnapshotPublication,
        *,
        migration_id: str,
    ) -> dict[str, Any]:
        """Acquire only metadata, then mount one writable COW for adoption."""

        volume_id = self._volume_id(reservation)
        try:
            result = self.client.get_volume(volume_id)
        except StorageNativeConflictError:
            result = None
        if result is None or self._record(result)["state"] == "deleted":
            result = self.client.acquire_snapshot(
                sandbox_id=reservation.sandbox_id,
                sandbox_generation=reservation.sandbox_generation,
                volume_id=volume_id,
                operation_id=f"quota-import:{migration_id}",
                publication=publication.to_dict(),
                accounting_id=reservation.project_id,
            )
        record = self._record(result)
        self._require_owner(record, reservation)
        if (
            record.get("published_manifest_digest")
            != publication.manifest_digest
        ):
            raise StorageNativeNodeError(
                "storage-native import owns another snapshot manifest"
            )
        if record["state"] == "published":
            result = self.client.mount_snapshot_cow(
                sandbox_id=reservation.sandbox_id,
                sandbox_generation=reservation.sandbox_generation,
                volume_id=volume_id,
                operation_id=f"quota-import-mount:{migration_id}",
                expected_revision=int(record["revision"]),
            )
            record = self._record(result)
        if record["state"] != "mounted":
            raise StorageNativeNodeError(
                f"imported storage-native volume is {record['state']}, not mounted"
            )
        expected_path = self.mount_root / volume_id
        if Path(record["mount_path"]) != expected_path:
            raise StorageNativeNodeError(
                "storage-native import returned an unexpected mount path"
            )
        return self._ready_payload(record, reservation)

    def inventory(self) -> tuple[dict[str, Any], ...]:
        raw = self.client.list_volumes().get("records")
        if not isinstance(raw, list):
            raise StorageNativeNodeError(
                "storage-native volume inventory is invalid"
            )
        result: list[dict[str, Any]] = []
        identities: set[tuple[str, int]] = set()
        projects: set[int] = set()
        for item in raw:
            if not isinstance(item, dict):
                raise StorageNativeNodeError(
                    "storage-native volume inventory contains a non-object"
                )
            if item.get("state") == "deleted":
                continue
            sandbox_id = str(item.get("sandbox_id") or "")
            sandbox_generation = int(item.get("sandbox_generation", -1))
            project_id = int(item.get("accounting_id", 0))
            virtual_size = int(item.get("virtual_size", 0))
            identity = (sandbox_id, sandbox_generation)
            if (
                not sandbox_id
                or sandbox_generation < 0
                or project_id <= 0
                or virtual_size <= 0
                or virtual_size % MIB
                or identity in identities
                or project_id in projects
            ):
                raise StorageNativeNodeError(
                    "storage-native volume inventory violates quota ownership"
                )
            identities.add(identity)
            projects.add(project_id)
            result.append(
                {
                    "hard_limit_mb": virtual_size // MIB,
                    "path": str(item["mount_path"]),
                    "project_id": project_id,
                    "sandbox_generation": sandbox_generation,
                    "sandbox_id": sandbox_id,
                    "state": "ready",
                }
            )
        return tuple(result)

    @staticmethod
    def _record(result: dict[str, Any]) -> dict[str, Any]:
        record = result.get("record")
        if not isinstance(record, dict):
            raise StorageNativeNodeError(
                "storage-native service returned an invalid record"
            )
        return record

    @staticmethod
    def _require_owner(
        record: dict[str, Any],
        reservation: HibernationDiskReservation,
    ) -> None:
        if (
            record.get("sandbox_id") != reservation.sandbox_id
            or record.get("sandbox_generation") != reservation.sandbox_generation
            or record.get("accounting_id") != reservation.project_id
            or record.get("virtual_size") != reservation.total_mb * MIB
        ):
            raise StorageNativeNodeError(
                "storage-native volume belongs to another reservation"
            )

    @staticmethod
    def _ready_payload(
        record: dict[str, Any],
        reservation: HibernationDiskReservation,
    ) -> dict[str, Any]:
        return {
            "hard_limit_mb": reservation.total_mb,
            "path": str(record["mount_path"]),
            "project_id": reservation.project_id,
            "sandbox_generation": reservation.sandbox_generation,
            "sandbox_id": reservation.sandbox_id,
            "state": "ready",
        }

    @staticmethod
    def _absent_payload(
        reservation: HibernationDiskReservation,
        *,
        removed: bool,
    ) -> dict[str, Any]:
        return {
            "project_id": reservation.project_id,
            "removed": removed,
            "sandbox_generation": reservation.sandbox_generation,
            "sandbox_id": reservation.sandbox_id,
            "state": "absent",
        }

    @staticmethod
    def _volume_id(reservation: HibernationDiskReservation) -> str:
        return (
            f"{reservation.sandbox_id}.sandbox-"
            f"{reservation.sandbox_generation}"
        )
