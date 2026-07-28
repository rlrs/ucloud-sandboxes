from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, TypeVar
from uuid import uuid4

from .direct_warden import (
    CommandResult,
    DirectRunscWarden,
    DirectSandbox,
    DirectWardenError,
)
from .hibernation import HibernationRecord, HibernationState


_T = TypeVar("_T")


@dataclass(frozen=True)
class DirectNodeCapability:
    """Fail-closed production gate for the direct-runsc lifecycle."""

    enabled: bool = False
    max_concurrent_restores: int = 8
    allowed_runsc_sha256: str = ""
    allowed_boot_config_sha256: str = ""

    def __post_init__(self) -> None:
        if self.max_concurrent_restores < 1:
            raise ValueError("max_concurrent_restores must be positive")
        if self.enabled and (
            len(self.allowed_runsc_sha256) != 64
            or len(self.allowed_boot_config_sha256) != 64
        ):
            raise ValueError(
                "enabled direct-node capability requires exact runtime fingerprints"
            )


class DirectNodeCoordinator:
    """One privileged node owner around all direct-runsc backends.

    Lifecycle mutations and tool execution are serialized per sandbox
    incarnation. Restores are separately bounded across the node so an
    inference-completion burst cannot create unbounded disk/page-in pressure.
    """

    def __init__(
        self,
        warden: DirectRunscWarden,
        capability: DirectNodeCapability | None = None,
    ) -> None:
        self.warden = warden
        self.capability = capability or DirectNodeCapability()
        runtime = warden.config.runtime_fingerprint
        if self.capability.enabled and (
            runtime.runsc_sha256 != self.capability.allowed_runsc_sha256
            or runtime.boot_config_sha256
            != self.capability.allowed_boot_config_sha256
        ):
            raise DirectWardenError(
                "direct-node capability does not match the qualified runtime"
            )
        self._restore_slots = asyncio.Semaphore(
            self.capability.max_concurrent_restores
        )
        self._incarnation_locks: dict[tuple[str, int], asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def create(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> HibernationRecord:
        self._require_enabled()
        async with await self._incarnation_lock(sandbox):
            return await self._settled_call(
                lambda: asyncio.to_thread(
                    self.warden.create,
                    sandbox,
                    operation_id=operation_id,
                )
            )

    async def exec(
        self,
        sandbox: DirectSandbox,
        argv: tuple[str, ...],
    ) -> CommandResult:
        """Wake once if needed, then execute the triggering tool call."""
        self._require_enabled()
        async with await self._incarnation_lock(sandbox):
            record = await self._settled_call(
                lambda: asyncio.to_thread(self.warden.inspect, sandbox)
            )
            if record is None:
                raise DirectWardenError("sandbox has no durable lifecycle record")
            if record.state not in {
                HibernationState.RUNNING,
                HibernationState.PARKED,
            }:
                record = await self._settled_call(
                    lambda: asyncio.to_thread(self.warden.reconcile, sandbox)
                )
            if record.state == HibernationState.PARKED:
                async with self._restore_slots:
                    record = await self._settled_call(
                        lambda: asyncio.to_thread(
                            self.warden.resume,
                            sandbox,
                            operation_id=f"wake:{uuid4().hex}",
                        )
                    )
            if record.state != HibernationState.RUNNING:
                raise DirectWardenError(
                    f"sandbox cannot accept tool traffic in state {record.state.value}"
                )
            return await self._settled_call(
                lambda: asyncio.to_thread(self.warden.exec, sandbox, argv)
            )

    async def park(
        self,
        sandbox: DirectSandbox,
        *,
        operation_id: str,
    ) -> HibernationRecord:
        self._require_enabled()
        async with await self._incarnation_lock(sandbox):
            return await self._settled_call(
                lambda: asyncio.to_thread(
                    self.warden.park,
                    sandbox,
                    operation_id=operation_id,
                )
            )

    async def reconcile(self, sandbox: DirectSandbox) -> HibernationRecord:
        self._require_enabled()
        async with await self._incarnation_lock(sandbox):
            return await self._settled_call(
                lambda: asyncio.to_thread(self.warden.reconcile, sandbox)
            )

    async def delete(self, sandbox: DirectSandbox) -> None:
        self._require_enabled()
        key = self._key(sandbox)
        async with await self._incarnation_lock(sandbox):
            await self._settled_call(
                lambda: asyncio.to_thread(self.warden.delete, sandbox)
            )
        async with self._locks_guard:
            lock = self._incarnation_locks.get(key)
            if lock is not None and not lock.locked():
                self._incarnation_locks.pop(key, None)

    async def _incarnation_lock(self, sandbox: DirectSandbox) -> asyncio.Lock:
        key = self._key(sandbox)
        async with self._locks_guard:
            return self._incarnation_locks.setdefault(key, asyncio.Lock())

    @staticmethod
    def _key(sandbox: DirectSandbox) -> tuple[str, int]:
        return sandbox.sandbox_id, sandbox.sandbox_generation

    async def _settled_call(
        self,
        create: Callable[[], Awaitable[_T]],
    ) -> _T:
        """Do not let caller cancellation abandon an in-flight mutation."""
        task = asyncio.create_task(create())
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await asyncio.shield(task)
            except Exception:
                # The caller still receives cancellation, while the next
                # operation reconciles any durable error state.
                pass
            raise

    def _require_enabled(self) -> None:
        if not self.capability.enabled:
            raise DirectWardenError("direct-node lifecycle capability is disabled")
