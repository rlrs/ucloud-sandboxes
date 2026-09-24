# Remaining wake latency: offline investigation after rc23

Scope: existing captured telemetry and local source/reproduction only. No production
commands, configuration changes, deployment, or workload were performed for this
investigation. The reproduction is a functional queue test with fake native
runtime fixtures on macOS, not a Linux performance measurement.

Previously captured production observations at approximately 07:13 UTC: eight rc23
workers, about 62–65 GiB available RAM per worker; completed-wake p95 about 2.7s,
30 completed wakes over 10s in five minutes; lifecycle-busy retries and a burst of
90 pending deliveries. PostgreSQL had no pool waiters in the sampled instant.
These snapshots do not locate the delay within a particular current request.

## Local fixes and validation

Both confirmed blocking paths below are now fixed locally; no production access
or deployment was performed. Continuation growth uses the existing memory ledger
and admission condition without a restore permit. Actual native restores retain
their permit, and startup admission, physical/backing checks, and durable wake
fencing are unchanged. Placement-cache reads/writes/removals use a separate short
lock; durable allocator operations retain their existing serialization and cache
publication remains after commit.

The updated reproduction completes the resident continuation while all eight
restore permits are still held (zero restore waiters). The original
`restore-slot-reproduction.json` is historical pre-fix evidence; the post-fix
result is `restore-slot-fixed.json` in the same directory.

Five new regression tests cover saturated restore permits, memory-blocked growth
leaving I/O permits free, duplicate continuation accounting, cached reads during
blocked allocator validation, and conservative reads during an uncommitted mode
change. The three blocking regressions also failed against unchanged HEAD,
confirming that they detect the old behavior. Existing tests cover failed commits,
restart, deletion/reimport, actual restore queuing, cancellation and backing limits.

Local functional suite: 93 tests, 92 passed. The remaining existing
`test_crossed_capture_requotes_full_growth_before_sparse_restore` fails its sparse
allocation assertion here (8,396,800 bytes vs expected <1 MiB), identically on
unchanged HEAD. This is a filesystem-dependent fixture limitation; no assertion
was weakened. Native Linux and production latency qualification remain outstanding.
`git diff --check` passed.

Command:

```sh
.venv/bin/python -m unittest tests.test_managed_growth tests.test_memory_backing_mode tests.test_ram_memory_backing tests.test_hybrid_memory_admission tests.test_split_memory_lifecycle tests.test_transition_admission tests.test_startup_admission tests.test_resident_reclaim_fence
```

The findings below describe the pre-fix paths and remaining investigation items.

## 1. Resident continuation waits behind disk-restore slots: reproduced

`DirectSandboxService._admit_managed_growth` acquires `_restore_slot` before
`_active_admission_guard`, including when the sandbox is already running and
needs no native restore. `_restore_admission` uses the same queue for actual disk
restores. Its default capacity is eight. Growth waiters keep their slot while
waiting for memory, so waiting operations can also occupy this entire pool.

The assembled-service reproduction creates a running managed guest, transitions
it to a safe wait, and holds the restore slots as if slow restores were running.
Its continuation needs 4 GiB with 6 GiB available (including the existing 2 GiB
admission reserve). It queues until a restore slot is released, then succeeds.
See `docs/benchmarks/wake-admission-investigation-2026-09-24/`.

Recommended correction: memory-growth admission should use the existing byte
ledger and wait condition without acquiring an I/O restore slot. Actual restore
work still takes a restore permit. Preserve durable activation/wake fencing,
cancellation, physical/RAM-backing guarantees, and fairness for queued owners.
Do not replace the eight-slot gate with another arbitrary resident-wake cap.

Acceptance: saturate actual restores and separately saturate memory-waiting
continuations; a resident continuation that fits must progress, while one that
does not fit remains queued. Verify cancellation and no duplicate activation.

## 2. Cached placement reads inherit allocator I/O lock: confirmed source path

`MemoryBackingStore.active_mode` acquires `self._lock`, also used by `prepare`,
`prepare_file_restore`, and parts of checkpoint retirement/deletion. Those
mutation sections perform filesystem validation and SQLite work/commit.
`_refresh_growth_forecasts_locked` calls `_growth_cost` →
`warden.application_memory_mode` → `active_mode` while holding the service's
node-wide `_capacity_guard`. A stalled mutation can therefore hold up otherwise
cached reads and spread to unrelated admission and demand snapshots.

Recommended correction: isolate the tiny cached placement map behind its own
short lock or immutable snapshot. Durable placement mutation remains serialized
by its existing locks. Publish a new cached mode only after the durable commit;
RAM-to-file placement is monotonic per incarnation, so an older RAM observation
is conservative. Preserve deletion/reimport identity and restart behavior.

Acceptance: pause an allocation validation/commit in a fixture, and demonstrate
that cached placement reads and unrelated admission still progress. Test failed
commits, deletion/reimport, and old readers without granting unsafe file credit.
The current production duration attributable to this lock is not measured.

## 3. rc23 only mitigates capture bursts; it cannot make a started capture cheap

A persistent forecast-only deficit now admits one reclaim candidate at a time.
A one-second grace avoids short bursts, but once capture starts the guest's wake
still joins the lifecycle transition. Serial probes can produce a slow tail if
captures take tens of seconds. More delay or blindly relaxing memory guarantees
would not solve that bottleneck.

Prioritize ready resident continuations whose actual growth reservation fits,
then re-evaluate whether any remaining safe wait must be checkpointed. Preserve
existing wake fences; never cancel a partially committed checkpoint casually.
Longer-term cheap reclaim is separate work and remains unqualified/off main.

## 4. Additional suspects requiring measurements, not assumptions

- Resident samples expire after 2.5s. Sampling walks all managed guests serially
  and waits another second before the next sweep. If a sweep slows sufficiently,
  observed residency loses credit and growth forecasts revert toward full limits.
  Measure sample age, sweep duration, and forecast increase before changing this.
  Refresh stale evidence outside admission locks; do not count stale RAM as free.
- Health requests open the registry-usage SQLite database and validate schema
  synchronously. The observed 1.5–1.9s health responses may include storage or
  request scheduling; no evidence here proves gateway CPU saturation. Time those
  phases before deciding to cache readiness evidence or change resources.
- “sandbox lifecycle is busy” alone does not identify the lock holder. rc23's new
  growth-admission and lifecycle-wait spans can distinguish major phases, but
  actual restore permit wait and allocator-cache lock wait need direct attribution.

Implement and qualify items 1 and 2 first. Then run a synchronized response burst
with small resident heaps and mixed 4/8 GiB limits while keeping agents active.
Measure resident versus parked wakes separately, slot waiting, admission waiting,
lifecycle waiting, capture time, and completion after the burst. Current evidence
supports these improvements but does not prove they explain every production tail.
