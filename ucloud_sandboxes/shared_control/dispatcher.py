from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import logging

from .model import StateConflict, WakeOperation, WakeProof, positive_seconds
from .postgres import PostgresControlStore


class WakeDispatcher:
    """Claim durable work, then release all DB connections before calling workers.

    Multiple processes may run this loop. A crashed dispatcher leaves a claim
    that expires; the next one retries the same operation, retaining reservations.
    The callback must enforce worker operation/sequence identity, not perform an
    unfenced generic wake. The deployed worker protocol is not wired here yet.
    """

    def __init__(
        self, store: PostgresControlStore,
        wake: Callable[[WakeOperation], Awaitable[WakeProof]], *,
        concurrency: int = 16, claim_seconds: float = 30,
        rpc_seconds: float = 10, retry_seconds: float = .1,
        on_completed: Callable[[WakeOperation], None] | None = None,
    ) -> None:
        if concurrency < 1 or positive_seconds(claim_seconds) <= positive_seconds(rpc_seconds):
            raise ValueError("positive dispatch concurrency and claim longer than RPC required")
        self.store, self.wake = store, wake
        self.concurrency, self.claim_seconds, self.rpc_seconds = concurrency, claim_seconds, rpc_seconds
        self.retry_seconds = positive_seconds(retry_seconds)
        self.on_completed = on_completed
        self.completed = self.retried = 0

    async def _dispatch(self, operation: WakeOperation) -> None:
        if not await self.store.prepare_dispatch(operation, retry_seconds=self.retry_seconds):
            return
        try:
            proof = await asyncio.wait_for(self.wake(operation), self.rpc_seconds)
        except (OSError, asyncio.TimeoutError):
            # An uncertain RPC outcome keeps its capacity reservation. Cancellation
            # intentionally leaves the claim to expire instead of guessing failure.
            await self.store.retry(operation, delay_seconds=self.retry_seconds)
            self.retried += 1
            return
        if not isinstance(proof, WakeProof):
            raise StateConflict("worker callback did not supply a fenced wake proof")
        if await self.store.complete(operation, proof):
            self.completed += 1
            if self.on_completed is not None:
                try:
                    self.on_completed(operation)
                except Exception:
                    logging.getLogger(__name__).exception("wake completion observer failed")

    async def run_once(self) -> int:
        operations = await self.store.claim_due(limit=self.concurrency, lease_seconds=self.claim_seconds)
        results = await asyncio.gather(*(self._dispatch(op) for op in operations), return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result
        return len(operations)

    async def run(self, stop: asyncio.Event, *, idle_seconds: float = .05) -> None:
        positive_seconds(idle_seconds)
        running: set[asyncio.Task] = set()
        try:
            while not stop.is_set():
                for task in tuple(running):
                    if task.done():
                        running.remove(task)
                        task.result()
                available = self.concurrency - len(running)
                if available:
                    operations = await self.store.claim_due(limit=available, lease_seconds=self.claim_seconds)
                    running.update(asyncio.create_task(self._dispatch(op)) for op in operations)
                # Refill on each completion; waiting for a whole batch creates a
                # head-of-line barrier even when most RPC slots are idle.
                if running:
                    await asyncio.wait(running, timeout=idle_seconds, return_when=asyncio.FIRST_COMPLETED)
                else:
                    try:
                        await asyncio.wait_for(stop.wait(), idle_seconds)
                    except asyncio.TimeoutError:
                        pass
        finally:
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)
