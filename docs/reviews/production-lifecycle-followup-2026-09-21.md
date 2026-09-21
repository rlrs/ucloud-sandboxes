# Production lifecycle follow-up, 21 September 2026

The latest retained non-test workload ran **13:32:58–14:01:02 UTC** on **0.5.70**. It produced 3,603 program requests across 208 distinct sandbox IDs. Later activity was deployment testing. Production was idle when inspected, with no sandbox routes or fresh worker heartbeats. Current deployed runtime is **0.5.73**; these historical traces cannot establish its loaded performance.

Evidence: [retained metrics and selected traces](../benchmarks/production-lifecycle-followup-2026-09-21.json). Searches covered 13:00–14:02 UTC, returning up to 30 traces per category; the selected slow traces are a diagnostic sample, not a representative latency distribution.

## Largest unresolved delay: coordination before worker execution

Twenty-minute histogram estimates ending 13:59 UTC put relay wake p95 at **97.0 seconds**, worker wake at **17.4 seconds**, relay park at **56.4 seconds**, and worker park at **13.9 seconds**. Separate histograms cannot be subtracted to estimate individual request overhead, but linked traces show the gap directly:

| Trace | Response POST | Before gateway wake starts | Gateway wake | Worker wake |
|---|---:|---:|---:|---:|
| `de9cdbb76d871c40175ab92ffb22656` | 102.94 s | 61.33 s | 41.61 s | 3.04 s |
| `5c7693f4c092108a06d3001e169aa995` | 104.91 s | 69.39 s | 35.52 s | 0.012 s |
| `53c865dbb0c580ea1825114209b34f79` | 76.43 s | 75.49 s | 0.94 s | 0.015 s |

The first trace did not begin its gateway-to-node call until about 97.09 seconds after the response POST started. That is roughly 35.76 seconds inside the gateway before the worker call. The latter two worker endpoints were effectively no-ops: almost all latency was elsewhere. Another trace (`3087a7229f16bf00080e6552f1635fe1`) contains two capacity retries before success.

The trace data does **not** distinguish all of the preceding lifecycle-lock wait, relay admission wait, placement-lock wait, other database waits, and capacity retries. Program `response_ready_at` is recorded when the gateway observes the lifecycle notification; it is not the model result's commit timestamp. Thread-CPU duration on an async relay span also includes unrelated event-loop work during its awaits, so it is not a per-request CPU profile.

Already deployed changes cancel queued/backoff park operations once a model response is committed, reduce wake placement inventory scans, and avoid publication without an eligible destination. An in-flight park must still finish before wake; breaking that ordering risks lifecycle corruption. This follow-up adds:

- `relay.lifecycle.lock_wait_seconds` to existing park/wake spans.
- A `gateway.wake.reserve_placement` span with `gateway.placement.lock_wait_seconds`. Its remaining duration shows reservation work under the locks. Both process and file locks, and all admission checks, are retained.

Together with the dispatcher's existing admission timing, these let a new loaded run separate important causes rather than treating all waiting as slow restore.

Create retries also showed gateway delay: traces `175166be27d9a056040c00504a27df44`, `3ad3fee0d9af716301f89016a86317a3`, and `150f97240f7d3e2808d651cd9f888764` took approximately **5.6–6.4 seconds**, while worker inventory headers arrived in **40–58 ms**. These were retries of an existing generation, not distinct slow container boots. The current evidence does not identify the wait responsible, so no speculative change to allocation locking was made.

## Avoidable gateway work fixed locally

**Lifecycle transport epochs scanned all migration history.** The production database retained 268 completed migrations. Every lifecycle reply loaded and decoded all of them before filtering for one sandbox. The reply now uses the existing sandbox-ID index. It still includes completed handoffs and retains generation/create-operation filtering, preserving reconnect/reset semantics.

**Ordinary database reads repeatedly wrote permission metadata.** Routing connection entry/exit and control-state transaction entry unconditionally called `chmod` on the database, WAL, and shared-memory files. They now inspect the mode and change it only when needed. Permissions are checked on every access: new sidecars and externally changed permissions are still repaired. No cached authorization decision or durability setting was introduced.

A reproducible local synthetic benchmark uses 268 migration records, one relevant to the requested sandbox, with empty snapshot metadata. See [results](../benchmarks/lifecycle-metadata-2026-09-21.json) and `scripts/benchmark_lifecycle_metadata.py`. These are small hot-path improvements, not an explanation for tens of seconds of waiting or a production speedup measurement.

For 100 reads with live WAL/SHM files, routing permission writes fell **600 → 0**, and control-state permission writes fell **300 → 0**. The indexed epoch lookup decodes one migration instead of 268; exact local latency measurements are in the JSON.

## Storage still needs loaded remeasurement

Worker release-storage p95 was approximately **5.39 seconds**. In two retained park traces:

- `936f893a20600ded06c69173382d764a`: storage release admission waited **2.63 seconds** within a roughly four-second RPC.
- `25216ae8b49c4b3a12768343cc005121`: the release RPC took about **3.9 seconds**, with effectively zero admission wait.

Both queueing and actual release work mattered in the old run. Raising concurrency alone could amplify storage contention. The deployed publication, compaction, retired-device, and inventory changes target this work; they require a new loaded run before choosing another invasive storage change.

## Validation and rollout state

Regression coverage checks completed migration history and transport epochs, per-sandbox lifecycle lookups, permissions on live and recreated SQLite sidecars, and park-before-wake ordering with timing recorded. **1,051 server tests passed (six skipped)**, along with Ruff and `git diff --check`. The follow-up was subsequently deployed as **0.5.74**; see the [rollout verification](release-0.5.74-production-2026-09-21.md). No SDK or Verifiers change is required for these backend changes.
