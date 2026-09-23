"""Typed in-flight transition evidence, owned by the direct service's admission lock.

This is accounting, not another scheduler. The service's FIFO queues, lifecycle
fences and live-pressure checks remain the authority for running operations.
"""

from dataclasses import dataclass, replace
from enum import Enum

from .models import ResourceQuantity


class TransitionKind(str, Enum):
    STARTUP = "startup"
    RESTORE = "restore"
    CAPTURE = "capture"
    PUBLICATION = "publication"


@dataclass(frozen=True)
class MemoryDemand:
    physical_bytes: int = 0
    ram_backing_bytes: int = 0


@dataclass(frozen=True)
class TransitionCost:
    kind: TransitionKind
    memory_bytes: int | None
    read_bytes: int | None = None
    write_bytes: int | None = None
    provenance: str = "unknown"
    ram_backing_bytes: int | None = None

    def __post_init__(self):
        if not isinstance(self.kind, TransitionKind):
            raise ValueError("invalid transition kind")
        for name in ("memory_bytes", "read_bytes", "write_bytes", "ram_backing_bytes"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"invalid transition {name}")


@dataclass(frozen=True)
class TransitionClaim:
    token: int
    owner: tuple[str, int]
    cost: TransitionCost
    requested: ResourceQuantity | None = None


class TransitionLedger:
    """One exact claim lifetime; callers hold the service's capacity guard.

    Unknown byte quantities stay unknown. I/O volume never becomes an invented
    bandwidth limit. Duplicate request owners still get independent tokens, so a
    cancellation cannot release another operation's claim.
    """

    def __init__(self):
        self._next = 0
        self._claims: dict[int, TransitionClaim] = {}
        self._waiting: dict[int, TransitionClaim] = {}
        self._growth: dict[tuple[str, int], TransitionCost] = {}

    def wait(self, owner, cost):
        self._next += 1
        self._waiting[self._next] = TransitionClaim(self._next, owner, cost)
        return self._next

    def unwait(self, token):
        self._waiting.pop(token, None)

    def refresh_wait_cost(self, owner, cost):
        for token, item in self._waiting.items():
            if item.owner == owner and item.cost.kind == cost.kind:
                self._waiting[token] = replace(item, cost=cost)

    def claim(self, owner, cost, *, requested=None):
        self._next += 1
        result = TransitionClaim(self._next, owner, cost, requested)
        self._claims[result.token] = result
        return result

    def active_count(self, kind):
        return sum(item.cost.kind == kind for item in self._claims.values())

    def release(self, claim):
        if self._claims.get(claim.token) != claim:
            raise ValueError("transition claim was not owned")
        self._claims.pop(claim.token)

    def resource_reservations(self):
        """Configured shapes retained for the existing heartbeat/drain contract."""
        return {
            item.owner: item.requested
            for item in self._claims.values()
            if item.requested is not None
        }

    def set_growth_forecasts(self, forecasts):
        """Residual primary-process growth; durable ownership stays in registry."""
        self._growth = dict(forecasts)

    def _memory_by_owner(self, resource="memory_bytes"):
        owners = {owner: getattr(cost, resource) for owner, cost in self._growth.items()
                  if getattr(cost, resource) is not None}
        for claim in self._claims.values():
            value = getattr(claim.cost, resource)
            if value is not None:
                owners[claim.owner] = max(
                    owners.get(claim.owner, 0), value
                )
        return owners

    @property
    def known_memory_bytes(self):
        return sum(self._memory_by_owner().values())

    def projected_memory_bytes(self, owner, cost, *, restore_capacity=0, resource="memory_bytes"):
        owners = self._memory_by_owner(resource)
        value = getattr(cost, resource)
        if value is not None:
            owners[owner] = max(owners.get(owner, 0), value)
        if cost.kind == TransitionKind.STARTUP:
            # Do not spend the headroom a queued continuation needs merely to
            # start another empty sandbox/primary. This reserves no execution
            # slot: both may proceed immediately when their combined cost fits.
            pending = self._next_pending(TransitionKind.RESTORE, restore_capacity)
            if pending is not None and getattr(pending.cost, resource) is not None:
                owners[pending.owner] = max(
                    owners.get(pending.owner, 0), getattr(pending.cost, resource)
                )
        return sum(owners.values())

    def _pending(self, kind):
        active = {
            item.owner for item in self._claims.values() if item.cost.kind == kind
        }
        owners = {}
        for item in self._waiting.values():
            if item.cost.kind == kind and item.owner not in active:
                owners.setdefault(item.owner, item)
        return list(owners.values())

    @property
    def foreground_waiting(self):
        return any(
            self._pending(kind)
            for kind in (TransitionKind.STARTUP, TransitionKind.RESTORE)
        )

    def _next_pending(self, kind, capacity):
        if self.active_count(kind) >= capacity:
            return None
        return next(iter(self._pending(kind)), None)

    def next_memory_demands(self, limits, *, resource="memory_bytes"):
        """Reclaim for progress, not for filling every concurrency slot.

        Already admitted bytes remain fully charged. Only the next eligible
        FIFO owner needs additional headroom to unblock progress. Admission can
        still grant every operation that fits; each grant updates these claims
        before another decision. Existing continuations precede new launches.
        """
        owners = self._memory_by_owner(resource)
        for kind in (TransitionKind.RESTORE, TransitionKind.STARTUP):
            item = self._next_pending(kind, limits.get(kind, 0))
            if item is not None:
                value = getattr(item.cost, resource)
                if value is not None:
                    owners[item.owner] = max(
                        owners.get(item.owner, 0), value
                    )
                break
        return sum(owners.values())

    def demand_snapshot(self, limits):
        admitted = self.known_memory_bytes
        unknown = sum(item.cost.memory_bytes is None for item in self._claims.values())
        unknown += sum(cost.memory_bytes is None for cost in self._growth.values())
        unknown += sum(
            item.cost.memory_bytes is None
            for kind in limits for item in self._pending(kind)
        )
        return {
            "admitted_demand_bytes": admitted,
            "pending_demand_bytes": max(0, self.next_memory_demands(limits) - admitted),
            "unknown_transition_memory_costs": unknown,
            "admitted_ram_backing_bytes": sum(self._memory_by_owner("ram_backing_bytes").values()),
            "pending_ram_backing_bytes": max(0, self.next_memory_demands(limits, resource="ram_backing_bytes")
                                             - sum(self._memory_by_owner("ram_backing_bytes").values())),
        }

    def snapshot(self):
        result = {}
        for kind in TransitionKind:
            costs = [
                item.cost for item in self._claims.values() if item.cost.kind == kind
            ]
            result[kind.value] = {
                "active": len(costs),
                "waiting": len(self._pending(kind)),
                "memory_bytes": None
                if any(cost.memory_bytes is None for cost in costs)
                else sum(cost.memory_bytes for cost in costs),
                "read_bytes": None
                if any(cost.read_bytes is None for cost in costs)
                else sum(cost.read_bytes for cost in costs),
                "write_bytes": None
                if any(cost.write_bytes is None for cost in costs)
                else sum(cost.write_bytes for cost in costs),
            }
        return result
