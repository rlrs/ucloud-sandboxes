from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable

from hypothesis import settings, strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from tests.test_storage_native_daemon import (
    FakeBlockBackend,
    FakeHost,
    FakePublisher,
)
from ucloud_sandboxes.storage_native_daemon import (
    StorageNativeConflictError,
    StorageNativeNodeConfig,
    StorageNativeNodeService,
    StorageNativePendingOperation,
    StorageVolumeRecord,
    StorageVolumeState,
)
from ucloud_sandboxes.storage_native_registry import (
    PublishedStorageLayer,
    StorageSnapshotPublication,
)


class StorageNativeServiceStateMachine(RuleBasedStateMachine):
    """Model one live authority plus durable tombstones across restarts."""

    capacity_unit = 1 << 20
    capacity = 3 * capacity_unit

    def __init__(self) -> None:
        super().__init__()
        self._temporary_directory = TemporaryDirectory()
        self.root = Path(self._temporary_directory.name)
        self.global_config = self.root / "global.json"
        self.global_config.write_text("{}\n", encoding="ascii")
        self.config = StorageNativeNodeConfig(
            journal_path=self.root / "journal" / "storage.sqlite",
            runtime_root=self.root / "runtime",
            mount_root=self.root / "mounts",
            hard_capacity_bytes=self.capacity,
            device_pool_enabled=True,
        )
        self.backend = FakeBlockBackend(descriptor=True, pooled=True)
        self.host = FakeHost(self.backend)
        self.publisher = FakePublisher()
        self.service = self._new_service()
        self.model: dict[str, StorageVolumeRecord] = {}
        self.initial_kind: dict[str, str] = {}
        self.accounting_ids: list[int] = []
        self.current: StorageVolumeRecord | None = None
        self.retired: list[StorageVolumeRecord] = []
        self.next_volume = 1
        self.operation_sequence = 0

    def teardown(self) -> None:
        self._temporary_directory.cleanup()

    def _new_service(self) -> StorageNativeNodeService:
        return StorageNativeNodeService(
            self.config,
            backend=self.backend,
            global_config_path=self.global_config,
            host=self.host,
            publisher=self.publisher,
        )

    def _restart(self) -> None:
        self.service = self._new_service()
        assert {item.volume_id: item for item in self.service.journal.list()} == (
            self.model
        )

    def _operation_id(self, kind: str) -> str:
        self.operation_sequence += 1
        return f"{kind}-{self.operation_sequence}"

    @staticmethod
    def _index(record: StorageVolumeRecord) -> int:
        return int(record.volume_id.removeprefix("volume-"))

    def _publication(
        self,
        index: int,
        virtual_size: int,
    ) -> StorageSnapshotPublication:
        return StorageSnapshotPublication(
            manifest_digest="sha256:" + f"{index + 100:064x}",
            tag=f"snapshot-{index}",
            repository="snapshots",
            repo_blob_url="http://registry/v2/snapshots/blobs",
            virtual_size=virtual_size,
            layers=(
                PublishedStorageLayer(
                    digest="sha256:" + f"{index:064x}",
                    size=4096,
                ),
            ),
        )

    @staticmethod
    def _expect_conflict(call: Callable[[], object]) -> None:
        try:
            call()
        except StorageNativeConflictError:
            return
        raise AssertionError("storage operation unexpectedly crossed its fence")

    def _effects(self) -> tuple[object, ...]:
        return (
            self.backend.create_calls,
            self.backend.restack_calls,
            tuple(self.backend.release_calls),
            tuple(self.backend.delete_calls),
            frozenset(self.host.mounted),
            frozenset(self.host.frozen),
            tuple(self.host.detached),
        )

    def _call_initial(
        self,
        index: int,
        kind: str,
        virtual_size: int,
        operation_id: str | None = None,
    ) -> StorageVolumeRecord:
        fields = {
            "sandbox_id": f"sandbox-{index}",
            "sandbox_generation": index,
            "volume_id": f"volume-{index}",
        }
        if kind == "create":
            return self.service.create_volume(
                **fields,
                operation_id=operation_id or f"create-{index}",
                virtual_size=virtual_size,
            )
        return self.service.acquire_snapshot(
            **fields,
            operation_id=operation_id or f"import-{index}",
            publication_raw=self._publication(index, virtual_size).to_dict(),
        )

    def _call_transition(
        self,
        kind: str,
        record: StorageVolumeRecord,
        operation_id: str,
        *,
        expected_revision: int | None = None,
    ) -> StorageVolumeRecord:
        method = {
            "delete": self.service.delete_volume,
            "mount": self.service.mount_snapshot_cow,
            "release": self.service.release_runtime,
            "seal": self.service.freeze_and_seal,
        }[kind]
        return method(
            sandbox_id=record.sandbox_id,
            sandbox_generation=record.sandbox_generation,
            volume_id=record.volume_id,
            operation_id=operation_id,
            expected_revision=(
                record.revision if expected_revision is None else expected_revision
            ),
        )

    def _transition_and_replay(
        self,
        kind: str,
        before: StorageVolumeRecord,
        expected_state: StorageVolumeState,
    ) -> StorageVolumeRecord:
        operation_id = self._operation_id(kind)
        result = self._call_transition(kind, before, operation_id)
        assert result.revision == before.revision + 1
        assert result.state == expected_state
        self.current = result
        self.model[result.volume_id] = result
        effects = self._effects()
        self._restart()
        assert self._call_transition(kind, before, operation_id) == result
        assert self._effects() == effects
        return result

    @rule(size_units=st.integers(min_value=1, max_value=4), imported=st.booleans())
    @precondition(lambda self: self.current is None)
    def provision(self, size_units: int, imported: bool) -> None:
        index = self.next_volume
        kind = "import" if imported else "create"
        virtual_size = size_units * self.capacity_unit
        before = tuple(self.service.journal.list())
        if kind == "create" and virtual_size > self.capacity:
            self._expect_conflict(lambda: self._call_initial(index, kind, virtual_size))
            assert self.service.journal.list() == before
            return

        record = self._call_initial(index, kind, virtual_size)
        assert record.state == (
            StorageVolumeState.PUBLISHED if imported else StorageVolumeState.MOUNTED
        )
        if self.accounting_ids:
            assert record.accounting_id > self.accounting_ids[-1]
        self.accounting_ids.append(record.accounting_id)
        self.initial_kind[record.volume_id] = kind
        self.model[record.volume_id] = record
        self.current = record
        self.next_volume += 1
        effects = self._effects()
        self._restart()
        assert self._call_initial(index, kind, virtual_size) == record
        assert self._effects() == effects

    @rule()
    @precondition(
        lambda self: (
            self.current is not None
            and self.current.state
            in {
                StorageVolumeState.MOUNTED,
                StorageVolumeState.SEALED,
                StorageVolumeState.RELEASED,
                StorageVolumeState.PUBLISHED,
            }
        )
    )
    def advance_lifecycle(self) -> None:
        assert self.current is not None
        before = self.current
        if before.state == StorageVolumeState.MOUNTED:
            self._transition_and_replay("seal", before, StorageVolumeState.SEALED)
            return
        if before.state == StorageVolumeState.SEALED:
            self._transition_and_replay("release", before, StorageVolumeState.RELEASED)
            return
        if before.state == StorageVolumeState.PUBLISHED:
            reserved = int(self.service.metrics()["hard_reserved_bytes"])
            if reserved + before.virtual_size > self.capacity:
                operation_id = self._operation_id("mount")
                self._expect_conflict(
                    lambda: self._call_transition("mount", before, operation_id)
                )
                assert self.service.journal.load(before.volume_id) == before
                return
        self._transition_and_replay("mount", before, StorageVolumeState.MOUNTED)

    @rule(failure=st.sampled_from(("none", "release", "unmount")))
    @precondition(
        lambda self: (
            self.current is not None
            and self.current.state
            in {
                StorageVolumeState.MOUNTED,
                StorageVolumeState.SEALED,
                StorageVolumeState.RELEASED,
                StorageVolumeState.PUBLISHED,
                StorageVolumeState.ERROR,
            }
        )
    )
    def delete(self, failure: str) -> None:
        assert self.current is not None
        before = self.current
        operation_id = self._operation_id("delete")
        release_failure = failure == "release" and before.device_id is not None
        if release_failure:
            self.backend.fail_next_release = True
            try:
                self._call_transition("delete", before, operation_id)
            except StorageNativePendingOperation:
                pass
            else:
                raise AssertionError("injected backend release unexpectedly succeeded")
            pending = self.service.journal.load(before.volume_id)
            assert pending is not None
            assert pending.state == StorageVolumeState.DELETING
            assert pending.revision == before.revision + 1
            self.current = pending
            self.model[pending.volume_id] = pending
            self._restart()
            self._expect_conflict(
                lambda: self._call_transition("delete", before, operation_id)
            )
            return
        if failure == "unmount" and Path(before.mount_path) in self.host.mounted:
            self.host.fail_next_unmount = True
        deleted = self._call_transition("delete", before, operation_id)
        assert deleted.state == StorageVolumeState.DELETED
        assert deleted.revision == before.revision + 1
        self.model[deleted.volume_id] = deleted
        self.retired.append(deleted)
        self.current = None
        effects = self._effects()
        self._restart()
        assert self._call_transition("delete", before, operation_id) == deleted
        assert self._effects() == effects

    @rule()
    @precondition(
        lambda self: (
            self.current is not None
            and self.current.state
            not in {StorageVolumeState.DELETING, StorageVolumeState.DELETED}
        )
    )
    def reject_stale_revision_and_owner(self) -> None:
        assert self.current is not None
        before = tuple(self.service.journal.list())
        self._expect_conflict(
            lambda: self._call_transition(
                "delete",
                self.current,
                self._operation_id("stale"),
                expected_revision=self.current.revision - 1,
            )
        )
        assert self.service.journal.list() == before
        wrong_owner = replace(
            self.current,
            sandbox_generation=self.current.sandbox_generation + 1,
        )
        self._expect_conflict(
            lambda: self._call_transition(
                "delete", wrong_owner, self._operation_id("wrong-owner")
            )
        )
        assert self.service.journal.list() == before

    @rule()
    @precondition(lambda self: bool(self.retired))
    def deleted_records_do_not_resurrect(self) -> None:
        deleted = self.retired.pop()
        index = self._index(deleted)
        kind = self.initial_kind[deleted.volume_id]
        self._restart()
        assert (
            self._call_initial(
                index,
                kind,
                deleted.virtual_size,
            )
            == deleted
        )
        self._expect_conflict(
            lambda: self._call_transition(
                "delete", deleted, self._operation_id("after-delete")
            )
        )
        assert self.service.journal.load(deleted.volume_id) == deleted
        if kind != "import":
            return

        retry_operation = self._operation_id("retry-import")
        retried = self._call_initial(
            index,
            kind,
            deleted.virtual_size,
            retry_operation,
        )
        assert retried.state == StorageVolumeState.PUBLISHED
        assert retried.accounting_id == deleted.accounting_id
        assert retried.revision == deleted.revision + 1
        self.current = retried
        self.model[retried.volume_id] = retried
        effects = self._effects()
        self._restart()
        assert (
            self._call_initial(
                index,
                kind,
                deleted.virtual_size,
                retry_operation,
            )
            == retried
        )
        assert self._effects() == effects

    @rule()
    def reconcile(self) -> None:
        before = dict(self.model)
        self.service.reconcile()
        after = {record.volume_id: record for record in self.service.journal.list()}
        for volume_id, previous in before.items():
            reconciled = after[volume_id]
            if previous.state == StorageVolumeState.DELETING:
                assert reconciled.state == StorageVolumeState.DELETED
                assert reconciled.revision == previous.revision
                self.retired.append(reconciled)
                self.current = None
            else:
                assert reconciled == previous
        self.model = after
        self._restart()

    @rule()
    def restart(self) -> None:
        self._restart()

    @invariant()
    def storage_authority_invariants_hold(self) -> None:
        records = self.service.journal.list()
        assert {record.volume_id: record for record in records} == self.model
        accounting_ids = [record.accounting_id for record in records]
        assert len(accounting_ids) == len(set(accounting_ids))
        assert set(accounting_ids) == set(self.accounting_ids)
        assert self.accounting_ids == sorted(self.accounting_ids)
        assert int(self.service.metrics()["hard_reserved_bytes"]) <= self.capacity

        owners = {
            owner.owner_id: owner for owner in self.backend.list_runtime_device_owners()
        }
        assert len({owner.device_id for owner in owners.values()}) == len(owners)
        for record in records:
            owner = owners.get(record.device_owner_id)
            if record.device_owner_id:
                assert owner is not None
                assert owner.device_id == record.device_id
                assert record.device_id in self.backend.live
            if record.state in {
                StorageVolumeState.MOUNTED,
                StorageVolumeState.SEALED,
            }:
                assert owner is not None
                assert Path(record.mount_path) in self.host.mounted
            if record.state == StorageVolumeState.DELETED:
                assert not record.device_owner_id
                assert record.device_id is None
                assert Path(record.mount_path) not in self.host.mounted


TestStorageNativeServiceStateMachine = StorageNativeServiceStateMachine.TestCase
TestStorageNativeServiceStateMachine.settings = settings(
    max_examples=12,
    stateful_step_count=18,
    deadline=None,
    derandomize=True,
)
