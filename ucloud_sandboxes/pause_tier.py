"""Pause tier (C1.1): stop idle sandboxes in place, reclaim them under pressure.

A pause changes no ownership: the Warden journal stays RUNNING/LIVE, the route
stays running, and the Warden thaws before any runsc exec. Nothing here grants
lifecycle authority. Its state is disposable; Warden pause markers are truth.

`runsc pause` stops the guest's tasks inside the Sentry. It does not freeze the
cgroup (2026-10-02 qualification: `frozen 0`), and the Warden does not write
cgroup.freeze either: it changed no reclaim measurably, and a frozen cgroup must
be thawed before every runsc resume and exec, a second state that every Warden
path and crash window would have to undo first.
"""

from contextlib import contextmanager
from dataclasses import dataclass
import errno
import os
from pathlib import Path
import threading
import time

# Gray's five-minute rule for DRAM against NVMe: keeping a byte resident for
# about five minutes costs as much as writing it out and reading it back.
TRANSFER_BREAK_EVEN_SECONDS = 300.0
# One node-wide reclaim budget (qualification: one reclaim moves 225-380 MiB/s
# with zswap off; eight at once share about 0.85 GiB/s, and with zswap they are
# CPU-bound at 21 MiB/s each). Two in flight at one shared rate keep a single
# sandbox's speed and leave disk for captures and pulls. A window is one write;
# 16 MiB windows halved one reclaim's rate (105 against 188 MB/s, benchmarks/
# pause-reclaim-2026-10-03), so a window is 128 MiB, still cancellable between.
RECLAIM_CONCURRENCY = 2
RECLAIM_BYTES_PER_SECOND = 512 * 1024**2
RECLAIM_WINDOW_BYTES = 128 * 1024**2
# A reclaim freeing less than this (or less than its whole target) made no
# progress. A wait retries after a doubling backoff and escalates to hibernate
# only after MAX_STALLS such reclaims in a row: one unlucky window is no proof
# that swap cannot take the wait (DSec: hibernate is for offload).
STALL_BYTES = 16 * 1024**2
STALL_BACKOFF_SECONDS = 10.0
MAX_STALLS = 2
# zswap keeps incompressible pages nearly 1:1 in a pool charged to the same
# cgroup, so a reclaim through it frees nothing (1,568 MB moved, 32 MB freed:
# benchmarks/pause-reclaim-2026-10-03). When zswap is on, each paused cgroup
# may hold at most this share of its memory bound there; the kernel sends the
# rest to swap. A fixed bound, whatever the guest's memory compresses to.
ZSWAP_SHARE_OF_BOUND = 0.25
# Free swap below this share is the kernel's: reclaim stops short of it and
# the rest of the deficit escalates to hibernate.
SWAP_RESERVE_FRACTION = 0.10
ESCALATION_CONCURRENCY = 2
# Thaw prefetch (qualification: 4 MiB pieces over 8 threads swapped 645 MiB
# back in 0.81-0.95 s, where the guest's own faults took 3.6-5.0 s; eight thaws
# with 64 readers reached 1.5 GiB/s together). A thaw waits for at most 1 GiB
# (about 1.3 s alone) and 2 s; demand faults bring back the rest, in
# proportion to what the guest touches.
PREFETCH_THREADS = 8
PREFETCH_NODE_THREADS = 64
PREFETCH_PIECE_BYTES = 4 * 1024**2
PREFETCH_READ_BYTES = 1024**2
PREFETCH_MAX_BYTES = 1024**3
PREFETCH_SECONDS = 2.0
# Less swap than this is about the Sentry's and gofer's own heaps (15 MiB,
# which no memory-file read restores): nothing worth a prefetch moved out.
PREFETCH_MIN_SWAP_BYTES = 32 * 1024**2


def hibernate_beats_pause(expected_wait_seconds, resident_bytes, footprint_bytes):
    """Aries rule: idle seconds x resident bytes > capture + restore cost.

    The left side is the RAM a pause holds for the wait; reclaim lowers it. The
    right side prices capturing and restoring the whole footprint, swapped
    pages included, in the same RAM byte-seconds.
    """
    return (expected_wait_seconds is not None and footprint_bytes > 0
            and expected_wait_seconds * resident_bytes
            > TRANSFER_BREAK_EVEN_SECONDS * footprint_bytes)


def park_tier(trigger, *, expected_wait_seconds=None, resident_bytes=0, footprint_bytes=0):
    """'pause' or 'hibernate' for one park on a pause-tier node (pure).

    Explicit API parks (durable park, drain and offload moves) hibernate. The
    idle timer pauses; a model wait pauses unless the Aries rule prefers a
    hibernate for its predicted remaining wait.
    """
    if trigger not in {"idle", "relay", "explicit"}:
        raise ValueError("park trigger is invalid")
    if trigger == "explicit" or (trigger == "relay" and hibernate_beats_pause(
            expected_wait_seconds, resident_bytes, footprint_bytes)):
        return "hibernate"
    return "pause"


def advised_wait_seconds(phase, wall_now):
    """Remaining model wait that a fresh relay phase hint predicts, else None."""
    if (phase is None or phase.get("phase") != "model_wait"
            or phase.get("expected_remaining_wait_seconds") is None
            or wall_now >= phase["expires_at"]):
        return None
    return max(0.0, phase["expected_remaining_wait_seconds"]
               - max(0.0, wall_now - phase["evaluated_at"]))


@dataclass
class PausedWait:
    paused_at: float
    expected_until: float | None = None
    resident_bytes: int | None = None
    swapped_bytes: int = 0
    reclaiming: int = 0  # The target of its in-flight reclaim, in bytes.
    escalating: bool = False
    # Reclaims in a row that made no progress, and when the next may start.
    stalls: int = 0
    retry_at: float = 0.0
    # A node-local model wait's growth-forecast identity (observe_managed_wait).
    local_request_id: str = ""


def swap_room_bytes(pressure):
    """Swap that reclaim may still fill above the kernel's reserve.

    Unknown swap is unbounded: reclaim tries, and a stalled one escalates.
    """
    total, free = pressure.swap_total_bytes, pressure.swap_free_bytes
    if total is None or free is None:
        return float("inf")
    return max(0, free - int(total * SWAP_RESERVE_FRACTION))


def relief_plan(decision, paused, *, now, swap_room):
    """(reclaims, escalations) of paused waits that cover a deficit (pure).

    Only measured pressure (`decide_resident_wait`: headroom or PSI) acts.
    Evict the waits that will stay idle longest: hinted waits by expected
    remaining idle x resident bytes, then the rest by resident bytes, most
    recently paused first. Without a hint, a long-paused wait is the likeliest
    to wake next, and evicting it buys a thaw instead of relief.

    A wait swaps out while swap has room above the kernel's reserve; one in
    its stall backoff waits. One that stalled MAX_STALLS times in a row, or
    that no longer fits in swap, escalates to a durable hibernate if it holds
    at least STALL_BYTES resident and more resident than swapped: a capture
    faults swapped pages back in before it frees anything. In-flight work
    counts against the deficit (a reclaim its target, a hibernate all it
    holds) and an in-flight reclaim's target against the room; unmeasured
    waits carry no credit.
    """
    if not decision.reclaim:
        return (), ()
    reclaiming = sum(wait.reclaiming for wait in paused.values())
    room = swap_room - reclaiming
    deficit = decision.target_bytes - reclaiming - sum(
        wait.resident_bytes or 0 for wait in paused.values() if wait.escalating)

    def score(item):
        wait, resident = item[1], item[1].resident_bytes or 0
        if wait.expected_until is not None:
            return (1, max(0.0, wait.expected_until - now) * resident)
        return (0, resident / (1.0 + max(0.0, now - wait.paused_at)))

    reclaims, escalations = [], []
    for key, wait in sorted(paused.items(), key=score, reverse=True):
        if deficit <= 0:
            break
        if wait.reclaiming or wait.escalating or not wait.resident_bytes:
            continue
        stalled = wait.stalls >= MAX_STALLS
        if not stalled and room >= STALL_BYTES:
            if now < wait.retry_at:
                continue  # Backing off after a stall; others may relieve meanwhile.
            freed = min(wait.resident_bytes, deficit, room)
            reclaims.append((key, freed))
            room -= freed
        elif wait.resident_bytes >= max(STALL_BYTES, wait.swapped_bytes):
            freed = wait.resident_bytes
            escalations.append(key)
        else:
            continue
        deficit -= freed
    return tuple(reclaims), tuple(escalations)


def reclaim_stalled(result, target_bytes):
    """A reclaim that raised, or freed less than STALL_BYTES (or its whole
    smaller target) without being superseded, made no progress."""
    return result is None or (result.reason != "superseded" and result.reclaimed_bytes
                              < min(target_bytes, STALL_BYTES))


def note_reclaim(wait, stalled, *, now):
    """Count a stall into its doubling backoff; progress clears both."""
    if stalled:
        wait.stalls += 1
        wait.retry_at = now + STALL_BACKOFF_SECONDS * 2 ** (wait.stalls - 1)
    else:
        wait.stalls, wait.retry_at = 0, 0.0


def cap_zswap(pid, *, proc_root=Path("/proc"), cgroup_root=Path("/sys/fs/cgroup"),
              zswap_enabled=Path("/sys/module/zswap/parameters/enabled")):
    """Bound a process's cgroup's zswap to ZSWAP_SHARE_OF_BOUND of its memory
    bound, once; the bytes set, or None (zswap off, no bound, already set)."""
    try:
        if zswap_enabled.read_text().strip() not in {"Y", "1"}:
            return None
        unified = [line[4:] for line in (proc_root / str(pid) / "cgroup").read_text().splitlines()
                   if line.startswith("0::/")]
        if len(unified) != 1 or {".", ".."} & set(unified[0].split("/")):
            return None
        cgroup = cgroup_root / unified[0]
        bound = (cgroup / "memory.max").read_text().strip()
        if bound == "max" or (cgroup / "memory.zswap.max").read_text().strip() != "max":
            return None
        cap = int(int(bound) * ZSWAP_SHARE_OF_BOUND)
        (cgroup / "memory.zswap.max").write_text(str(cap))
        return cap
    except (OSError, ValueError):
        return None  # Optional: without the cap a reclaim only frees less.


def _may_raise_priority():
    """Whether this process may lower a thread's nice value again (CAP_SYS_NICE)."""
    nice = os.getpriority(os.PRIO_PROCESS, 0)
    try:
        os.setpriority(os.PRIO_PROCESS, 0, nice - 1)
    except OSError:
        return False
    os.setpriority(os.PRIO_PROCESS, 0, nice)
    return True


class ReclaimBudget:
    """The node's one budget for paused reclaim: slots, a shared rate, priority.

    The scheduler keeps at most `concurrency` reclaims in flight. Each window
    reserves its bytes where the previous reservation ends (virtual
    scheduling), so concurrent reclaims split one rate instead of each
    getting it. A cancelled wait forfeits its reservation: at most one window.
    """

    def __init__(self, *, concurrency=RECLAIM_CONCURRENCY,
                 bytes_per_second=RECLAIM_BYTES_PER_SECOND, clock=time.monotonic, sleep=time.sleep):
        if concurrency < 1 or bytes_per_second <= 0:
            raise ValueError("reclaim budget must be positive")
        self.concurrency, self.bytes_per_second = concurrency, bytes_per_second
        self._clock, self._sleep = clock, sleep
        self._lock = threading.Lock()
        self._next = 0.0
        self._restorable = _may_raise_priority()

    def admit(self, amount, is_current):
        """Wait for this window's share of the rate; False once superseded."""
        with self._lock:
            start = max(self._clock(), self._next)
            self._next = start + amount / self.bytes_per_second
        while (remaining := start - self._clock()) > 0:
            if not is_current():
                return False
            self._sleep(min(remaining, 0.05))
        return True

    @contextmanager
    def background(self):
        """Run one kernel write at the lowest CPU weight (nice 19).

        memory.reclaim, and zswap compression, run in the writer's context.
        Only the write runs low: it releases the GIL, while a Python thread
        left at nice 19 holding the GIL would stall every node-agent thread.
        Not SCHED_IDLE: a starved writer can sit on kernel reclaim locks.
        Without CAP_SYS_NICE the priority could not be restored, so the write
        keeps it.
        """
        if not self._restorable:
            yield
            return
        before = os.getpriority(os.PRIO_PROCESS, 0)
        os.setpriority(os.PRIO_PROCESS, 0, 19)
        try:
            yield
        finally:
            os.setpriority(os.PRIO_PROCESS, 0, before)


def cgroup_swap_bytes(pid, *, proc_root=Path("/proc"), cgroup_root=Path("/sys/fs/cgroup")):
    """memory.swap.current of a process's unified cgroup, or None."""
    try:
        unified = [line[4:] for line in (proc_root / str(pid) / "cgroup").read_text().splitlines()
                   if line.startswith("0::/")]
        if len(unified) != 1 or {".", ".."} & set(unified[0].split("/")):
            return None
        return int((cgroup_root / unified[0] / "memory.swap.current").read_text())
    except (OSError, ValueError):
        return None


def memory_pieces(fd, *, budget_bytes=PREFETCH_MAX_BYTES, piece_bytes=PREFETCH_PIECE_BYTES):
    """(start, end) pieces of fd's data extents in file order, up to a budget.

    On tmpfs a swapped-out page is still data; a hole holds nothing to read.
    """
    pieces, offset, size = [], 0, os.fstat(fd).st_size
    while offset < size and budget_bytes > 0:
        try:
            start = os.lseek(fd, offset, os.SEEK_DATA)
        except OSError as exc:
            if exc.errno == errno.ENXIO:
                break  # Only a hole remains.
            raise
        offset = min(os.lseek(fd, start, os.SEEK_HOLE), start + budget_bytes)
        pieces += [(at, min(at + piece_bytes, offset)) for at in range(start, offset, piece_bytes)]
        budget_bytes -= offset - start
    return pieces


def prefetch(fd, pieces, *, slots, cancelled, threads=PREFETCH_THREADS):
    """Read pieces of fd back in parallel; the bytes read.

    Each reader holds one of the node's `slots` for its whole life, so however
    many thaws run, the node has at most that many readers and read buffers.
    A thaw waits for its first slot and then takes only free ones, up to
    `threads`. Readers stop between reads once `cancelled()` is true or one
    fails. This returns only after every reader has exited, so the caller may
    close fd; a reader's error is raised after that.
    """
    pending, guard, counts, errors = iter(pieces), threading.Lock(), [], []

    def reader():
        count = 0
        try:
            buffer = memoryview(bytearray(PREFETCH_READ_BYTES))
            while not errors and not cancelled():
                with guard:
                    piece = next(pending, None)
                if piece is None:
                    break
                at, end = piece
                while at < end and not cancelled():
                    read = os.preadv(fd, [buffer[:end - at]], at)
                    if not read:
                        break  # The file shrank: nothing more here.
                    at, count = at + read, count + read
        except OSError as exc:
            errors.append(exc)
        finally:
            counts.append(count)
            slots.release()

    readers = []
    while len(readers) < min(threads, len(pieces)) and not cancelled():
        if not (slots.acquire(blocking=False) if readers else slots.acquire(timeout=0.05)):
            if readers:
                break
            continue
        readers.append(threading.Thread(target=reader, name="thaw-prefetch", daemon=True))
        try:
            readers[-1].start()
        except RuntimeError:  # No thread to spare: fewer readers, never a failed thaw.
            readers.pop()
            slots.release()
            break
    for item in readers:
        item.join()
    if errors:
        raise errors[0]
    return sum(counts)


STOP_COUNTERS = {"target_reached": "pause_reclaim_target_reached",
                 "not_shrinking": "pause_reclaim_not_shrinking",
                 "partial_reclaim": "pause_reclaim_partial", "raised": "pause_reclaim_errors"}


class PauseStats:
    """Monotonic counters; pause and thaw happen in the Warden on any path."""

    def __init__(self):
        self._lock = threading.Lock()
        self._values = dict.fromkeys((
            "pauses", "thaws", "thaw_ms_total", "thaw_ms_max", "pause_reclaims",
            "pause_reclaimed_bytes", "pause_reclaim_ms_total",
            "pause_reclaim_cancellations", "pause_reclaim_stalls", "pause_escalations",
            "thaw_prefetches", "thaw_prefetched_bytes", "thaw_prefetch_ms_total",
            *STOP_COUNTERS.values()), 0)

    def add(self, **amounts):
        with self._lock:  # Exact sums; only the reported snapshot rounds.
            for name, amount in amounts.items():
                previous = self._values[name]
                self._values[name] = (max(previous, amount) if name.endswith("_max")
                                      else previous + amount)

    def stopped(self, reason):
        """Count why a pause reclaim ended (a thaw's is a cancellation)."""
        if reason != "superseded":
            self.add(**{STOP_COUNTERS.get(reason, "pause_reclaim_errors"): 1})

    def snapshot(self):
        with self._lock:
            return {name: round(value) for name, value in self._values.items()}
