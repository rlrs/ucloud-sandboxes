import asyncio
from dataclasses import replace
from pathlib import Path
import threading
import time
import unittest

from ucloud_sandboxes.direct_node import (
    DirectNodeCapability,
    DirectNodeCoordinator,
)
from ucloud_sandboxes.direct_warden import (
    CommandResult,
    DirectRunscWardenConfig,
    DirectSandbox,
    DirectWardenError,
)
from ucloud_sandboxes.hibernation import (
    HibernationAuthority,
    HibernationRecord,
    HibernationRuntimeFingerprint,
    HibernationState,
)


DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64


def record(
    state: HibernationState,
    *,
    revision: int = 1,
) -> HibernationRecord:
    authority = {
        HibernationState.RUNNING: HibernationAuthority.LIVE,
        HibernationState.PARKED: HibernationAuthority.PARKED,
        HibernationState.RESTORING: HibernationAuthority.PARKED,
    }[state]
    return HibernationRecord(
        sandbox_id="sandbox",
        sandbox_generation=1,
        spec_sha256=DIGEST_A,
        state=state,
        authority=authority,
        operation_kind=(
            "initialize" if state == HibernationState.RUNNING else "hibernate"
        ),
        operation_id="operation",
        revision=revision,
        updated_ns=time.time_ns(),
        hibernation_generation=1 if state != HibernationState.RUNNING else 0,
        manifest_sha256=DIGEST_B if state != HibernationState.RUNNING else "",
        sentry_pid=100 if state == HibernationState.RUNNING else None,
        sentry_start_time_ticks=200 if state == HibernationState.RUNNING else None,
    )


class FakeWarden:
    def __init__(self) -> None:
        runtime = HibernationRuntimeFingerprint(
            runsc_sha256=DIGEST_A,
            runsc_commit="d" * 40,
            platform="systrap",
            architecture="x86_64",
            page_size=4096,
            cpu_features_sha256=DIGEST_B,
            boot_config_sha256=DIGEST_C,
            rootfs_sha256=DIGEST_B,
        )
        self.config = DirectRunscWardenConfig(
            runsc=Path("/runsc"),
            runtime_root=Path("/run"),
            memory_root=Path("/memory"),
            bundle_root=Path("/bundles"),
            journal_root=Path("/journals"),
            artifact_root=Path("/artifacts"),
            runtime_fingerprint=runtime,
        )
        self.records: dict[str, HibernationRecord] = {}
        self.resume_calls = 0
        self.exec_calls = 0
        self.concurrent_restores = 0
        self.max_seen_restores = 0
        self.resume_gate: threading.Event | None = None
        self.resume_started = threading.Event()

    def inspect(self, sandbox: DirectSandbox) -> HibernationRecord | None:
        return self.records.get(sandbox.sandbox_id)

    def create(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> HibernationRecord:
        del operation_id
        result = record(HibernationState.RUNNING)
        self.records[sandbox.sandbox_id] = result
        return result

    def resume(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> HibernationRecord:
        del operation_id
        self.resume_calls += 1
        self.concurrent_restores += 1
        self.max_seen_restores = max(
            self.max_seen_restores,
            self.concurrent_restores,
        )
        self.resume_started.set()
        try:
            if self.resume_gate is not None:
                self.resume_gate.wait(timeout=5)
            result = replace(
                record(HibernationState.RUNNING),
                sandbox_id=sandbox.sandbox_id,
            )
            self.records[sandbox.sandbox_id] = result
            return result
        finally:
            self.concurrent_restores -= 1

    def reconcile(self, sandbox: DirectSandbox) -> HibernationRecord:
        result = replace(
            record(HibernationState.PARKED),
            sandbox_id=sandbox.sandbox_id,
        )
        self.records[sandbox.sandbox_id] = result
        return result

    def exec(
        self,
        sandbox: DirectSandbox,
        argv: tuple[str, ...],
    ) -> CommandResult:
        self.exec_calls += 1
        return CommandResult(argv, 0, stdout=sandbox.sandbox_id)

    def park(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> HibernationRecord:
        del operation_id
        result = replace(
            record(HibernationState.PARKED),
            sandbox_id=sandbox.sandbox_id,
        )
        self.records[sandbox.sandbox_id] = result
        return result

    def delete(self, sandbox: DirectSandbox) -> None:
        self.records.pop(sandbox.sandbox_id, None)


def sandbox(name: str = "sandbox") -> DirectSandbox:
    return DirectSandbox(
        sandbox_id=name,
        sandbox_generation=1,
        container_id=(name.encode("utf-8").hex() + "0" * 64)[:64],
        spec_sha256=DIGEST_A,
        rootfs_sha256=DIGEST_B,
        bundle=Path(f"/bundles/{name}"),
        memory_directory=f"{name}.memory",
    )


def enabled(*, slots: int = 8) -> DirectNodeCapability:
    return DirectNodeCapability(
        enabled=True,
        max_concurrent_restores=slots,
        allowed_runsc_sha256=DIGEST_A,
        allowed_boot_config_sha256=DIGEST_C,
    )


class DirectNodeTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_capability_fails_closed(self) -> None:
        coordinator = DirectNodeCoordinator(FakeWarden())  # type: ignore[arg-type]
        with self.assertRaisesRegex(DirectWardenError, "disabled"):
            await coordinator.exec(sandbox(), ("/bin/true",))

    async def test_duplicate_triggering_execs_restore_once(self) -> None:
        warden = FakeWarden()
        item = sandbox()
        warden.records[item.sandbox_id] = record(HibernationState.PARKED)
        coordinator = DirectNodeCoordinator(warden, enabled())  # type: ignore[arg-type]

        first, second = await asyncio.gather(
            coordinator.exec(item, ("/bin/true",)),
            coordinator.exec(item, ("/bin/ls",)),
        )

        self.assertEqual(warden.resume_calls, 1)
        self.assertEqual(warden.exec_calls, 2)
        self.assertEqual(first.stdout, item.sandbox_id)
        self.assertEqual(second.stdout, item.sandbox_id)

    async def test_restore_concurrency_is_globally_bounded(self) -> None:
        warden = FakeWarden()
        warden.resume_gate = threading.Event()
        coordinator = DirectNodeCoordinator(warden, enabled(slots=2))  # type: ignore[arg-type]
        items = [sandbox(f"sandbox-{index}") for index in range(4)]
        for item in items:
            warden.records[item.sandbox_id] = replace(
                record(HibernationState.PARKED),
                sandbox_id=item.sandbox_id,
            )

        tasks = [
            asyncio.create_task(coordinator.exec(item, ("/bin/true",)))
            for item in items
        ]
        deadline = time.monotonic() + 2
        while warden.concurrent_restores < 2 and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        self.assertEqual(warden.concurrent_restores, 2)
        self.assertEqual(warden.max_seen_restores, 2)
        warden.resume_gate.set()
        await asyncio.gather(*tasks)

    async def test_cancellation_waits_for_restore_to_settle(self) -> None:
        warden = FakeWarden()
        warden.resume_gate = threading.Event()
        item = sandbox()
        warden.records[item.sandbox_id] = record(HibernationState.PARKED)
        coordinator = DirectNodeCoordinator(warden, enabled())  # type: ignore[arg-type]
        triggering = asyncio.create_task(
            coordinator.exec(item, ("/bin/true",))
        )
        await asyncio.to_thread(warden.resume_started.wait, 2)

        triggering.cancel()
        await asyncio.sleep(0)
        self.assertFalse(triggering.done())
        warden.resume_gate.set()
        with self.assertRaises(asyncio.CancelledError):
            await triggering
        self.assertEqual(
            warden.records[item.sandbox_id].state,
            HibernationState.RUNNING,
        )
        self.assertEqual(warden.exec_calls, 0)

    async def test_runtime_fingerprint_mismatch_fails_startup(self) -> None:
        with self.assertRaisesRegex(DirectWardenError, "qualified runtime"):
            DirectNodeCoordinator(
                FakeWarden(),  # type: ignore[arg-type]
                replace(enabled(), allowed_runsc_sha256=DIGEST_B),
            )


if __name__ == "__main__":
    unittest.main()
