from __future__ import annotations

import asyncio
from dataclasses import dataclass
import math


class DatabaseAdmissionUnavailable(RuntimeError):
    """No connection acquired; this transaction has not executed BEGIN."""


async def cancel_until_done(*tasks: asyncio.Future) -> None:
    """Cancel until every task ends, then reap them.

    On Python 3.10 a timed wait inside psycopg can consume a cancellation that
    races readiness, and a loop then enters its next wait. asyncio.wait never
    consumes it, and transaction rollback is shielded, so repeating is safe.
    """
    pending = set(tasks)
    while pending:
        for task in pending:
            task.cancel()
        _, pending = await asyncio.wait(pending, timeout=0.1)
    await asyncio.gather(*tasks, return_exceptions=True)


def positive_seconds(value: float) -> float:
    if not math.isfinite(value) or value <= 0:
        raise ValueError("duration must be finite and positive")
    return value


@dataclass(frozen=True)
class TransactionSample:
    operation: str
    pool_wait_seconds: float
    transaction_seconds: float
    commit_seconds: float
    succeeded: bool
    lock_query_seconds: float = 0
