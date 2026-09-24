"""Select resident model waits to reclaim only when resources need it.

This policy grants no lifecycle authority. Callers must still hold their ordinary
safe-point, generation and activity fences before checkpointing a selected wait.
"""

from collections import OrderedDict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
import os
import statistics
import threading
import time

from .background_io import PressureSampler
from .transition_admission import MemoryDemand


@dataclass(frozen=True)
class ResidentWaitDecision:
    memory_reclaim: bool
    psi_reclaim: bool
    target_bytes: int
    reason: str
    backing_reclaim: bool = False

    @property
    def reclaim(self):
        return self.memory_reclaim or self.psi_reclaim


def decide_resident_wait(
    pressure, incoming, *, memory_reclaim=False, psi_reclaim=False
):
    """Pure resource decision; neither guest age nor its memory limit is a bill.

    MemAvailable already includes resident waits. Incoming is the next wave of
    foreground demand, not the configured limits of an arbitrary backlog.
    """
    fraction = pressure.memory_fraction
    total = pressure.memory_available_bytes / fraction if fraction > 0 else 0
    reserve = max(total * 0.05, min(2 * 1024**3, total * 0.10))
    backing = getattr(pressure, "memory_backing", None)
    backing_unknown = backing is not None and (
        backing.total_bytes is None or backing.available_bytes is None
    )
    spare = pressure.memory_available_bytes - incoming.physical_bytes
    threshold = reserve * (1.5 if memory_reclaim else 1.0)
    backing_spare = None if backing is None else (
        (0 if backing_unknown else backing.available_bytes) - incoming.ram_backing_bytes
    )
    backing_reclaim = backing is not None and (
        backing_unknown or backing_spare <= threshold
    )
    if total:
        memory_reclaim = spare <= threshold
    else:
        memory_reclaim = fraction <= (0.075 if memory_reclaim else 0.05) or bool(
            incoming.physical_bytes
        )
    memory_reclaim = memory_reclaim or backing_reclaim
    # Reclaim PSI can come from checkpoint I/O. More checkpoints amplify that
    # feedback loop; real memory/demand deficits must still make progress.
    psi_reclaim = pressure.io_stall < 20 and pressure.memory_stall >= (
        2 if psi_reclaim else 10
    )
    reason = (
        "memory_backing_unavailable"
        if backing_unknown
        else "queued_demand"
        if (incoming.physical_bytes or incoming.ram_backing_bytes) and memory_reclaim
        else "memory_backing_headroom"
        if backing_reclaim
        else "memory_headroom"
        if memory_reclaim
        else "memory_reclaim"
        if psi_reclaim
        else "storage_backpressure"
        if pressure.io_stall >= 20
        else "resident_headroom"
    )
    # Concurrency follows a byte deficit. PSI-only reclaim is a small probe;
    # saturated I/O shrinks the probe, without preventing foreground progress.
    target = max(1, reserve * 1.5 - spare) if memory_reclaim else max(1, total * 0.01)
    if backing_reclaim:
        target = max(target, reserve * 1.5 - backing_spare)
    if pressure.io_stall >= 20 and total:
        target = min(target, total * 0.05)
    return ResidentWaitDecision(
        memory_reclaim,
        psi_reclaim,
        int(target) if memory_reclaim or psi_reclaim else 0,
        reason,
        backing_reclaim,
    )


@dataclass
class CacheReclaimObservation:
    sentry_identity: tuple[int, int]
    refault_file_pages: int
    reclaimed_bytes: int
    avoid_until: float = 0.0


@dataclass
class WaitHistory:
    # Incarnation-local observations, not a workload timeout or an admission cap.
    waits: deque = field(default_factory=lambda: deque(maxlen=32))
    parks: deque = field(default_factory=lambda: deque(maxlen=32))
    wakes: deque = field(default_factory=lambda: deque(maxlen=32))


@dataclass(frozen=True)
class WakeObservation:
    history: WaitHistory
    started_at: float
    memory_bytes: int


@dataclass(frozen=True)
class WaitAdvice:
    sequence: int
    phase: str
    deadline: float
    expected_until: float | None


class WarmParkDeferred(RuntimeError):
    def __init__(self, seconds):
        super().__init__("relay park deferred for warm retention")
        self.seconds = seconds


class WarmParkPolicy:
    def __init__(self, pressure=None, *, demand=lambda: MemoryDemand()):
        self.pressure = pressure or PressureSampler().sample
        self.demand = demand
        self._lock = threading.Lock()
        self._pending = {}
        self._waiting_since = OrderedDict()
        self._response_ready_at = {}
        self._reclaiming = {}
        self._ram_reclaiming = {}
        self._ram_footprints = {}
        self._backing_reclaim = False
        self._io_backpressure = False
        self._parked = set()
        self._footprints = {}
        self._reclaim_bytes = 0
        self._retry_after = {}
        self._settle_until = 0.0
        self._memory_reclaim = False
        self._psi_reclaim = False
        self._reason = "resident_headroom"
        self._completed = 0
        self._cache_attempted = set()
        self._cache_inflight = set()
        self._cache_history = OrderedDict()
        self._cache_attempts = 0
        self._cache_reclaimed_bytes = 0
        self._cache_refault_backoffs = 0
        self._history = OrderedDict()
        self._transition_started = {}
        self._ranking = ()
        self._ranking_at = 0.0
        self._ranking_until = 0.0
        self._ranking_dirty = True
        self._phase_sequences = OrderedDict()
        self._wait_advice = OrderedDict()

    def observe_phase(self, key, payload):
        """An expiring ordering hint on an already generation-bound park call."""
        if payload is None:
            return
        from .relay_phase import transport_phase

        hint = transport_phase(payload)
        now, wall_now = time.monotonic(), time.time()
        scope = (self._incarnation(key), hint["registration_incarnation"])
        with self._lock:
            previous = self._phase_sequences.get(scope)
            if previous is not None and hint["sequence"] < previous.sequence:
                return
            if previous is None or hint["sequence"] > previous.sequence:
                remaining_ttl = min(
                    hint["ttl_seconds"], max(0, hint["expires_at"] - wall_now)
                )
                remaining = hint.get("expected_remaining_wait_seconds")
                previous = WaitAdvice(
                    hint["sequence"],
                    hint["phase"],
                    now + remaining_ttl,
                    (
                        None
                        if remaining is None
                        else now
                        + max(
                            0,
                            remaining - max(0, wall_now - hint["evaluated_at"]),
                        )
                    ),
                )
                self._phase_sequences[scope] = previous
            # Replays share the original monotonic deadline, even if wall time
            # moved backward. A newer non-wait phase invalidates older advice.
            self._phase_sequences.move_to_end(scope)
            while len(self._phase_sequences) > 4096:
                self._phase_sequences.popitem(last=False)
            self._wait_advice[key] = (scope, previous.sequence)
            self._wait_advice.move_to_end(key)
            while len(self._wait_advice) > 4096:
                self._wait_advice.popitem(last=False)
            self._ranking_dirty = True

    def _advised_remaining(self, key, now):
        reference = self._wait_advice.get(key)
        if reference is None:
            return None
        scope, sequence = reference
        hint = self._phase_sequences.get(scope)
        if (
            hint is None
            or hint.sequence != sequence
            or hint.phase != "model_wait"
            or hint.expected_until is None
            or now >= hint.deadline
        ):
            return None
        return max(0, hint.expected_until - now)

    @staticmethod
    def _incarnation(key):
        return key[:2] if isinstance(key, tuple) else key

    def _history_for(self, key):
        incarnation = self._incarnation(key)
        history = self._history.setdefault(incarnation, WaitHistory())
        self._history.move_to_end(incarnation)
        while len(self._history) > 4096:
            self._history.popitem(last=False)
        return history

    @staticmethod
    def _cost(observations, memory_bytes):
        if not observations:
            return None
        # Never extrapolate a larger heap from the latency of a tiny checkpoint.
        # Shrinking the heap does not prove fixed workspace/kernel work shrank.
        return max(
            statistics.median(seconds for seconds, _ in observations),
            statistics.median(seconds / size for seconds, size in observations)
            * memory_bytes,
        )

    def _candidate_rank(self, key, now):
        started = self._waiting_since[key]
        footprint = self._footprints.get(key, 0)
        history = self._history.get(self._incarnation(key))
        fallback = (1, 0, 0, -started)
        if history is None or footprint <= 0:
            return fallback
        park = self._cost(history.parks, footprint)
        wake = self._cost(history.wakes, footprint)
        if park is None or wake is None or park + wake <= 0:
            return fallback
        age = max(0, now - started)
        # Condition on the wait still being outstanding. An old wait must not
        # inherit a fresh wait's entire predicted duration. If no past wait
        # lasted this long, there is no evidence for profitable remaining time.
        remaining = self._advised_remaining(key, now)
        if remaining is None:
            if not history.waits:
                return fallback
            survivors = [duration - age for duration in history.waits if duration > age]
            remaining = statistics.median(survivors) if survivors else 0
        cost = park + wake
        if remaining > cost:
            return (2, footprint * (remaining - cost) / cost, 0, -started)
        # Ranking never vetoes reclaim: under a real deficit even short waits
        # progress, preferring the least transition cost per measured byte.
        return (0, remaining / cost, footprint / cost, -started)

    def _ranked_waits(self, now):
        # Many request threads inspect the same candidate set. Cache only the
        # disposable ordering for one existing maintenance interval; authority,
        # retry eligibility and in-flight byte credit are rechecked below.
        if self._ranking_dirty or not self._ranking_at <= now < self._ranking_until:
            self._ranking = tuple(
                sorted(
                    self._waiting_since,
                    key=lambda key: (
                        key not in self._response_ready_at,
                        self._candidate_rank(key, now),
                    ),
                    reverse=True,
                )
            )
            self._ranking_at = now
            self._ranking_until = min(
                (
                    hint.deadline
                    for hint in self._phase_sequences.values()
                    if hint.deadline > now
                ),
                default=now + 0.25,
            )
            self._ranking_until = min(self._ranking_until, now + 0.25)
            self._ranking_dirty = False
        return self._ranking

    def _set_footprint(self, key, memory_bytes, ram_bytes):
        # Byte admission uses this value immediately. Reordering waits for the
        # next cached maintenance interval, avoiding N sorts for N new samples.
        self._footprints[key] = memory_bytes
        self._ram_footprints[key] = ram_bytes

    def _needs_reclaim(self, pressure, incoming):
        with self._lock:
            decision = decide_resident_wait(
                pressure,
                incoming,
                memory_reclaim=self._memory_reclaim,
                psi_reclaim=self._psi_reclaim,
            )
            self._memory_reclaim = decision.memory_reclaim
            self._psi_reclaim = decision.psi_reclaim
            self._reason = decision.reason
            self._backing_reclaim = decision.backing_reclaim
            self._io_backpressure = pressure.io_stall >= 20
            self._reclaim_bytes = decision.target_bytes
            return decision.reclaim

    def _selected(self, key, now):
        # Called under _lock. Project in-flight releases against the deficit
        # so one stale sample cannot authorize every wait to checkpoint.
        if key in self._reclaiming:
            return True
        if now < self._settle_until:
            return False
        # A byte deficit is not evidence that saturated storage can finish more
        # concurrent captures. Let admitted reclaim drain; when none remains,
        # one candidate may still make progress despite lagging PSI samples.
        # Healthy storage retains the ordinary measured-byte parallelism.
        if self._io_backpressure and self._reclaiming:
            return False
        # A missing/stale footprint is not a one-byte reclaim. Probe it alone
        # and observe the resulting headroom before starting more I/O. This is
        # uncertainty handling, not a limit on measured parallel checkpoints.
        inflight = self._ram_reclaiming if self._backing_reclaim else self._reclaiming
        if any(value is None or (value <= 0 and not self._backing_reclaim) for value in inflight.values()):
            return False
        remaining = self._reclaim_bytes - sum(inflight.values())
        for candidate in self._ranked_waits(now):
            if remaining <= 0:
                break
            if (
                candidate in self._parked
                or candidate in self._reclaiming
                or self._retry_after.get(candidate, 0) > now
            ):
                continue
            footprint = (self._ram_footprints.get(candidate) if self._backing_reclaim
                         else self._footprints.get(candidate, 0))
            if self._backing_reclaim and footprint == 0:
                continue  # Known file cache cannot free this tmpfs allocation.
            if candidate == key:
                return (footprint is not None and footprint > 0) or not self._reclaiming
            if footprint is None or footprint <= 0:
                return False  # Await the earlier unknown-footprint probe.
            remaining -= footprint
        return False

    def parked(self, key):
        """Record completed reclaim; this is observation, not a park fence."""
        with self._lock:
            first = key not in self._parked
            if first:
                self._completed += 1
            # A concurrent wake may already have removed this request.
            if key in self._waiting_since:
                self._parked.add(key)
                transition = self._transition_started.get(key)
                if first and transition is not None and key not in self._cache_inflight:
                    started, footprint = transition
                    elapsed = time.monotonic() - started
                    if elapsed > 0 and footprint > 0:
                        self._history_for(key).parks.append((elapsed, footprint))
                        self._ranking_dirty = True

    def cache_reclaim_target(self, key, sample, *, application_file_backed=False):
        """One measured cache probe per wait, only for an actual byte deficit."""
        if sample is None:
            return 0
        pressure = self.pressure()
        if pressure.io_stall >= 20 and not application_file_backed:
            return 0
        now = time.monotonic()
        incoming = self.demand()
        with self._lock:
            decision = decide_resident_wait(
                pressure,
                incoming,
                memory_reclaim=self._memory_reclaim,
                psi_reclaim=self._psi_reclaim,
            )
            if (
                not decision.memory_reclaim
                # Clean host cache eviction cannot free unswappable tmpfs
                # blocks. A backing-space deficit needs a durable checkpoint.
                or decision.backing_reclaim
                or key in self._cache_attempted
                or key not in self._reclaiming
                or key not in self._waiting_since
            ):
                return 0
            incarnation = key[:2]
            previous = self._cache_history.get(incarnation)
            identity = (sample.sentry_pid, sample.sentry_start_time_ticks)
            if previous is not None and previous.sentry_identity != identity:
                self._cache_history.pop(incarnation)
                previous = None
            if previous is not None and not application_file_backed:
                # Repeatedly evicting a refaulted working set trades memory
                # pressure for read amplification. Let durable park handle a
                # continuing deficit while this cache gets a recovery window.
                refaulted = max(
                    0, sample.refault_file_pages - previous.refault_file_pages
                ) * os.sysconf("SC_PAGE_SIZE")
                if (
                    previous.reclaimed_bytes
                    and refaulted >= previous.reclaimed_bytes // 2
                ):
                    previous.avoid_until = now + 30.0
                    previous.reclaimed_bytes = 0
                    self._cache_refault_backoffs += 1
                if now < previous.avoid_until:
                    return 0
            remaining = decision.target_bytes - sum(
                value
                for candidate, value in self._reclaiming.items()
                if candidate != key
            )
            if application_file_backed:
                # A retained application heap is expected to refault on its next
                # turn. Flush only its owned active file before chunked reclaim;
                # the measured deficit, not a small generic cache probe, bounds
                # useful progress. Never count tmpfs pages as reclaimable.
                reclaimable = max(0, min(sample.current_bytes,
                                         sample.file_bytes - sample.shared_memory_bytes))
                target = min(remaining, reclaimable)
            else:
                target = min(remaining, sample.clean_file_bytes, 256 * 1024**2)
            if target < 16 * 1024**2:
                return 0
            self._cache_attempted.add(key)
            self._cache_inflight.add(key)
            # This attempt can only reclaim cache, not the full live footprint.
            self._reclaiming[key] = target
            self._ram_reclaiming[key] = 0
            self._cache_attempts += 1
            return target

    def record_cache_reclaim(self, key, sample, result):
        with self._lock:
            self._cache_inflight.discard(key)
            if key in self._reclaiming:
                self._reclaiming[key] = max(0, self._footprints.get(key, 0))
                self._ram_reclaiming[key] = self._ram_footprints.get(key)
            if result is None:
                return
            self._cache_reclaimed_bytes += result.reclaimed_bytes
            if result.reclaimed_bytes:
                incarnation = key[:2]
                self._cache_history[incarnation] = CacheReclaimObservation(
                    (sample.sentry_pid, sample.sentry_start_time_ticks),
                    sample.refault_file_pages,
                    result.reclaimed_bytes,
                )
                self._cache_history.move_to_end(incarnation)
                while len(self._cache_history) > 4096:
                    self._cache_history.popitem(last=False)

    def cache_reclaim_still_needed(self, key, *, application_file_backed=False):
        """Cancel later writeback/reclaim windows after foreground progress."""
        pressure = self.pressure()
        incoming = self.demand()
        with self._lock:
            if key not in self._cache_inflight or key not in self._waiting_since:
                return False
            decision = decide_resident_wait(
                pressure, incoming, memory_reclaim=self._memory_reclaim,
                psi_reclaim=self._psi_reclaim,
            )
            return (decision.memory_reclaim and not decision.backing_reclaim
                    and (pressure.io_stall < 20 or application_file_backed))

    def snapshot(self):
        with self._lock:
            return {
                "resident_waits": len(self._waiting_since) - len(self._parked),
                "checkpoint_inflight": len(self._reclaiming)
                - len(self._cache_inflight),
                "cache_reclaim_inflight": len(self._cache_inflight),
                "reclaim_target_bytes": self._reclaim_bytes,
                "projected_reclaim_bytes": sum(self._reclaiming.values()),
                "checkpoints_completed": self._completed,
                "reason": self._reason,
                "cache_reclaim_attempts": self._cache_attempts,
                "cache_reclaimed_bytes": self._cache_reclaimed_bytes,
                "cache_refault_backoffs": self._cache_refault_backoffs,
            }

    @contextmanager
    def defer(self, key, *, memory_bytes=0, ram_bytes=None, blocking=True):
        with self._lock:
            self._set_footprint(key, memory_bytes, ram_bytes)
            entry = self._pending.get(key)
            if entry is None:
                started = self._waiting_since.setdefault(key, time.monotonic())
                self._ranking_dirty = True
                # Selection metadata is disposable; lifecycle fences are durable
                # elsewhere. Bound metadata without limiting accepted parks.
                while len(self._waiting_since) > 4096:
                    expired, _ = self._waiting_since.popitem(last=False)
                    self._parked.discard(expired)
                    self._cache_attempted.discard(expired)
                    self._retry_after.pop(expired, None)
                    self._footprints.pop(expired, None)
                    self._ram_footprints.pop(expired, None)
                    self._wait_advice.pop(expired, None)
                    self._response_ready_at.pop(expired, None)
                entry = [threading.Event(), started, 0]
                self._pending[key] = entry
            entry[2] += 1
        event = entry[0]
        claimed = False
        try:
            # No sandbox lock, execution slot or storage reservation is held.
            while not event.is_set():
                reclaim = self._needs_reclaim(self.pressure(), self.demand())
                if reclaim:
                    with self._lock:
                        if (
                            self._selected(key, time.monotonic())
                            and key not in self._reclaiming
                        ):
                            self._reclaiming[key] = max(0, memory_bytes)
                            self._ram_reclaiming[key] = ram_bytes
                            self._transition_started[key] = (
                                time.monotonic(),
                                memory_bytes,
                            )
                            claimed = True
                            break
                if not blocking:
                    # Bounded transport retry, not a resident wait expiry.
                    raise WarmParkDeferred(0.25 if reclaim else 3.0)
                event.wait(0.05)
            try:
                yield event
            except BaseException:
                if claimed:
                    with self._lock:
                        self._retry_after[key] = time.monotonic() + 1.0
                raise
        finally:
            with self._lock:
                if claimed:
                    self._reclaiming.pop(key, None)
                    self._ram_reclaiming.pop(key, None)
                    self._transition_started.pop(key, None)
                    self._cache_inflight.discard(key)
                    if key not in self._parked and key in self._waiting_since:
                        self._retry_after[key] = time.monotonic() + 1.0
                    self._settle_until = time.monotonic() + 0.25
                entry[2] -= 1
                if not entry[2]:
                    self._pending.pop(key, None)

    def ready(self, key, *, memory_bytes=0, ram_bytes=None):
        """Cheap local pressure check; it grants no lifecycle authority."""
        with self._lock:
            started = self._waiting_since.get(key)
            self._set_footprint(key, memory_bytes, ram_bytes)
        if started is None or not self._needs_reclaim(
            self.pressure(), self.demand()
        ):
            return False
        with self._lock:
            return self._selected(key, time.monotonic())

    def waiting(self, key):
        with self._lock:
            return key in self._waiting_since

    def response_ready(self, key):
        """Prefer other safe waits while this exact response awaits admission.

        This neither fences nor cancels capture: when every wait has a response,
        reclamation must still progress. Successful growth admission retains the
        existing durable wake fence before the wait is actually cancelled.
        """
        with self._lock:
            if key in self._waiting_since:
                self._response_ready_at.setdefault(key, time.monotonic())
                self._ranking_dirty = True

    def observed_after_wait(self, key, sampled_at):
        """A startup sample cannot price a heap grown before the safe wait."""
        with self._lock:
            started = self._waiting_since.get(key)
            return started is not None and sampled_at >= started

    def forget(self, key):
        self._finish(key)

    def wake(self, key):
        return self._finish(key, completed=True)

    def record_wake(self, key, observation):
        """Call only after a successful wake; a failed restore is not a sample."""
        if observation is None:
            return
        with self._lock:
            if self._history.get(self._incarnation(key)) is not observation.history:
                return  # Deletion/eviction revoked this disposable observation.
            elapsed = time.monotonic() - observation.started_at
            if elapsed > 0 and observation.memory_bytes > 0:
                observation.history.wakes.append((elapsed, observation.memory_bytes))
                self._ranking_dirty = True

    def forget_incarnation(self, sandbox_id, generation):
        incarnation = (sandbox_id, generation)
        with self._lock:
            self._history.pop(incarnation, None)
            self._cache_history.pop(incarnation, None)
            for scope in tuple(self._phase_sequences):
                if scope[0] == incarnation:
                    self._phase_sequences.pop(scope)
            for key in tuple(self._waiting_since):
                if self._incarnation(key) == incarnation:
                    self._finish_locked(key)
            self._ranking_dirty = True

    def _finish(self, key, *, completed=False):
        with self._lock:
            observation = None
            started = self._waiting_since.get(key)
            if completed and started is not None:
                now = time.monotonic()
                elapsed = max(0, self._response_ready_at.get(key, now) - started)
                history = self._history_for(key)
                history.waits.append(elapsed)  # Excludes our admission/restore delay.
                if key in self._parked:
                    observation = WakeObservation(
                        history, now, self._footprints.get(key, 0)
                    )
            self._finish_locked(key)
            return observation

    def _finish_locked(self, key):
        self._waiting_since.pop(key, None)
        self._parked.discard(key)
        self._cache_attempted.discard(key)
        self._retry_after.pop(key, None)
        self._footprints.pop(key, None)
        self._ram_footprints.pop(key, None)
        self._wait_advice.pop(key, None)
        self._response_ready_at.pop(key, None)
        self._ranking_dirty = True
        entry = self._pending.get(key)
        if entry is not None:
            entry[0].set()
