# 0.5.114 release closeout

> Historical candidate ledger. See [current production status](current-production-status.md)
> for the deployed rc22 baseline and the separately deferred changes.

Status: release withheld. After reviewing the pressure results, the user explicitly
reopened the original performance plan: cheap memory reclamation with a live runtime
and demand-paged wake at the physical RAM limit are required. The narrower
correctness-only closeout below is historical, not the current completion gate.
The immutable-environment adapter remains opt-in.

## Resulting behavior

- Managed model waits remain resident while resources permit. RAM-backed active
  application memory avoids forcing ordinary guest writes through checkpoint
  storage; durable checkpoints bind memory and workspace in one completion.
- PostgreSQL is the sole live relay backend. Durable response acceptance is
  independent of wake/delivery, while leases and retained delivery obligations
  preserve recovery and idempotency.
- Gateway wake placement and lifecycle commits have explicit, transport-independent
  boundaries. Exec events and small file uploads use one asynchronous response owner.
- Managed-primary growth is admitted at launch and continuation, not only when an
  empty sandbox is created. Its durable forecast survives ambiguous RPCs/restart;
  queued continuations leave resident waits eligible for pressure parking.
- Admission and the resident policy account for both physical memory and verified
  unswappable backing capacity. Missing configured evidence cannot mean unlimited
  capacity. See [the growth contract](../managed-growth-admission.md).

## Qualification

The rc12 pressure-256 trial recorded 16 guest SIGBUS exits and failed correctness.
No host OOM was recorded, and no contemporaneous backing-space sample survived;
the exact kernel fault cause remains unproven. Final pressure qualification must
reproduce the same 256 × 1.5GiB working-set workload and record both host and
backing headroom. Do not replace it with a resident-only test.

Earlier resident-512 candidates completed all 4,096 turns without parking or
operation errors, but did not achieve the subsecond latency goal. rc13 repository-256
passed 2,048 turns with live SQLite WAL/reader state; forced repository-16 passed 64
turns and 48 measured parks with state integrity preserved. These are supporting
gates, not substitutes for the final pressure and sustained runs.

The frozen rc18 package passed **1,635 Linux tests** in 212.733 seconds against
real PostgreSQL and SDK 0.4.26, with 12 expected environment skips. Its PostgreSQL
and resident-policy paths also passed **117 Python 3.10 tests** in 17.275 seconds.
The previously prepared, withheld 0.5.114 artifact used identical rc18 server
Python source. It is superseded by the current hybrid-memory work and must not
be deployed as that implementation. The new candidate and its loaded results
require their own qualification. See [the pressure-path design and ownership
contract](hybrid-memory-pressure-2026-09-23.md).

The pressure trials deliberately retain their failures:

| Candidate | Result and correction |
| --- | --- |
| rc15 | 61 cycles; a queued primary's pre-dispatch admission timeout escaped without retry metadata. rc16 uses the existing safe startup retry contract at that boundary. |
| rc16 | Three cycles; a transient managed-status timeout was caught as a semantic conflict (409). rc17 fixes exception ordering and verifies real HTTP retries with both SDK clients. |
| rc17 | Zero cycles; PostgreSQL pool acquisition escaped as 500. Seventeen simultaneous checkpoints also timed out on one worker. rc18 provides narrowly safe poll/respond backpressure and admits unknown capture footprints singly, with samples required to postdate the safe wait. |
| rc18, original budget | 502 cycles and 136 observed parks; one primary launch exceeded its 180-second SDK deadline. No guest signals were recorded, but 28 fleet-resource health checks failed. PostgreSQL suffered a measured 34-second WAL durability stall; cleanup completed. This is a failed pressure run. |

The rc18 retry named `pressure_correctness256` keeps the same 256 sandboxes,
eight cycles, 1.5 GiB heap, 384 MiB dirty working set and integrity checks. Its
per-sandbox SDK request budget is 1,800 seconds instead of 180, bounded by the
existing workload deadline. Cycle and probe deadlines are unchanged. This tests
eventual completion under queued pressure; it cannot qualify short request latency.

That longer-launch-budget repeat also failed, after **1,282 cycles**: one guest
continuation exceeded its unchanged 180-second deadline. All fleet checks passed
and cleanup was clean. The durable wake succeeded 180.308 seconds after response
commit; the initial wake alone spent 48.65 seconds on the worker before returning
retryable 503. PostgreSQL's shorter stalls do not explain that delay. No SIGBUS was
observed; this still is not a successful pressure qualification.

Review also identified a workload-end flaw: completed primaries slept forever
retaining their heap, even when the total completed heaps would exceed physical
RAM. The corrected harness terminates completed work and requires successful primary
exit without weakening per-cycle allocation, integrity or continuation checks.
Its 29 Linux tests passed, including signaled, malformed, nonzero, wrong-job and
missing terminal results; the guest fixture verifies SQLite state and natural
exit. The original 180-second pressure repeat with this fix also failed after 713
cycles: a first-cycle continuation timed out before any primary completed. Two
stale-heartbeat checks failed; cleanup was clean. This confirms that workload-end
retention does not explain the earlier continuation latency. Crash/integrity
qualification is therefore measured separately using the existing 1,800-second
overall workload budget, while preserving the failed original deadline results.

Detailed failed-run evidence and regression history remain in the
[completion audit](performance-architecture-completion-audit-2026-09-23.md),
[implementation ledger](performance-architecture-implementation-2026-09-23.md),
[worker memory evidence](../benchmarks/architecture-load-2026-09-23/rc18-pressure-memory/README.md)
and [PostgreSQL wait evidence](../benchmarks/relay-pool-pressure-2026-09-23/README.md).
The exact cause of the virtual disk's long write-completion latency is unproven;
ample disk space and low completed write volume do not establish a provider fault.

The separated crash/integrity run (`candidate18-integrity-pressure256`) completed
**2,048/2,048 cycles and 256/256 successful primary exits**, with no operation or
cleanup errors and no SIGBUS. Minimum sampled physical/backing headroom was
7.83/9.09 GiB. It observed parking on 155 measured cycles. This is a bounded
memory-correctness result, not an overall healthy-performance pass: useful-action
p95 was **53.94 seconds**, and 11 fleet-health entries failed (three stale
heartbeats and eight node entries from two failed resource probes). The request
and continuation budgets were explicitly 1,800 seconds; original pressure
failures remain retained. See the [report](../benchmarks/architecture-load-2026-09-23/architecture-candidate18-integrity-pressure256.summary.json).

The sustained512 baseline failed after 1,915 cycles at exec start with an HTML
“Job is unavailable | UCloud” 503. Cleanup succeeded. The earlier measurements
and prepared final artifacts do not qualify the reopened performance work.
Source commit and final deployment remain pending.
The final wheel and both worker-role bundles are prepared from the qualified
server source; immutable environments remain disabled.

## Rollout and compatibility

Gateway readers must precede workers emitting the new backing metrics/reasons.
Workers require the qualified split-memory native runtime and unswappable tmpfs
capability. Worker ownership schema 3/4 migrates transactionally to 5; an old binary
cannot reopen schema 5. Stateful rollback requires a compatible reader, or draining
and replacing the worker. Preserve durable growth forecasts.

The default rootfs adapter remains Docker. Immutable environments are opt-in and
were disabled in the effective production configuration during closeout. Their
broader fleet qualification is deferred by explicit user agreement.

SDK 0.4.26 is already published and Verifiers pins its immutable wheel; these
server-side memory-admission changes require no further SDK update. See
[coordinated release verification](sdk-verifiers-release-readiness-2026-09-23.md).

Memory growth is a forecast based on fresh observations, not an absolute guarantee
against arbitrary allocation while supposedly waiting or free/regrow between
samples. No claim of unrestricted memory overcommit or an achieved subsecond 512
latency SLO is made.
